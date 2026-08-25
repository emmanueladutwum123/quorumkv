// Package sim provides a deterministic cluster simulator and a linearizability
// checker for the histories it produces.
//
// The two halves work together. The simulator runs a whole cluster inside one
// goroutine under a seeded scheduler, so any interleaving it finds is
// reproducible from its seed. The checker then decides whether what the clients
// observed was actually possible — because a store can pass every hand-written
// assertion and still return an impossible answer under an interleaving nobody
// thought to write a test for.
package sim

import (
	"fmt"
	"sort"
	"strings"
)

// OpKind is a client operation.
type OpKind uint8

const (
	OpPut OpKind = iota
	OpGet
	OpDelete
	OpCAS
)

func (k OpKind) String() string {
	switch k {
	case OpPut:
		return "put"
	case OpGet:
		return "get"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	default:
		return fmt.Sprintf("op(%d)", uint8(k))
	}
}

// Op is one client operation, recorded with the interval over which it was in
// flight.
//
// The interval is what makes linearizability checkable at all. An operation may
// be placed anywhere inside its own interval, so concurrent operations can be
// ordered freely while non-overlapping ones cannot — that constraint is precisely
// what distinguishes linearizability from mere sequential consistency.
type Op struct {
	ClientID int
	Kind     OpKind
	Key      string

	// Value is the argument: the new value for a put, or the replacement for a
	// compare-and-swap.
	Value string
	// Expected and ExpectAbsent are the compare-and-swap precondition.
	Expected     string
	ExpectAbsent bool

	// Observed results.
	GotValue string
	GotFound bool
	Swapped  bool
	Existed  bool

	// Unknown marks an operation whose outcome the client never learned — a
	// timeout, or a lost leader. Such an operation may have taken effect or not,
	// and the checker must consider both, since forcing either would report
	// violations that are not there.
	Unknown bool

	// Invoke and Return are logical times. Return is 0 for an operation that never
	// returned, which the checker treats as still in flight at the end of the
	// history.
	Invoke int64
	Return int64
}

func (o Op) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "c%d %s %s", o.ClientID, o.Kind, o.Key)
	switch o.Kind {
	case OpPut:
		fmt.Fprintf(&b, "=%q", o.Value)
	case OpCAS:
		if o.ExpectAbsent {
			fmt.Fprintf(&b, " if-absent -> %q", o.Value)
		} else {
			fmt.Fprintf(&b, " if=%q -> %q", o.Expected, o.Value)
		}
	}
	b.WriteString(" => ")
	switch {
	case o.Unknown:
		b.WriteString("UNKNOWN")
	case o.Kind == OpGet && o.GotFound:
		fmt.Fprintf(&b, "%q", o.GotValue)
	case o.Kind == OpGet:
		b.WriteString("(absent)")
	case o.Kind == OpCAS:
		fmt.Fprintf(&b, "swapped=%v", o.Swapped)
	case o.Kind == OpDelete:
		fmt.Fprintf(&b, "existed=%v", o.Existed)
	default:
		b.WriteString("ok")
	}
	fmt.Fprintf(&b, "  [%d,%d]", o.Invoke, o.Return)
	return b.String()
}

// History is a recorded set of client operations.
type History struct {
	Ops []Op
}

// Add appends an operation.
func (h *History) Add(op Op) { h.Ops = append(h.Ops, op) }

// Len reports how many operations were recorded.
func (h *History) Len() int { return len(h.Ops) }

// keyState is the sequential specification of one key: a value that is either
// present or not.
type keyState struct {
	value   string
	present bool
}

// apply runs one operation against a state, reporting whether the observed
// result was possible and what the state becomes.
func (s keyState) apply(op Op) (keyState, bool) {
	switch op.Kind {
	case OpPut:
		next := keyState{value: op.Value, present: true}
		// A put has no interesting output, so any observed result is consistent.
		return next, true

	case OpGet:
		if op.Unknown {
			return s, true
		}
		// A read must return exactly what this state holds. This is the check that
		// catches a stale read: a value that was correct earlier but is not correct
		// at any position the read's interval allows.
		if op.GotFound != s.present {
			return s, false
		}
		if s.present && op.GotValue != s.value {
			return s, false
		}
		return s, true

	case OpDelete:
		next := keyState{}
		if op.Unknown {
			return next, true
		}
		if op.Existed != s.present {
			return s, false
		}
		return next, true

	case OpCAS:
		shouldSwap := false
		if op.ExpectAbsent {
			shouldSwap = !s.present
		} else {
			shouldSwap = s.present && s.value == op.Expected
		}
		next := s
		if shouldSwap {
			next = keyState{value: op.Value, present: true}
		}
		if op.Unknown {
			return next, true
		}
		// Two clients must not both observe a successful create of the same key.
		// That is caught here: only one position in any sequential order can find
		// the key absent.
		if op.Swapped != shouldSwap {
			return s, false
		}
		return next, true

	default:
		return s, false
	}
}

// Result reports whether a history was linearizable, with enough detail to
// diagnose a failure.
type Result struct {
	OK bool
	// Key is the key whose sub-history could not be linearized.
	Key string
	// Ops is that sub-history, in invocation order.
	Ops []Op
	// Reason describes what went wrong.
	Reason string
	// Inconclusive reports that the search for some key was abandoned before it
	// finished. The history was not shown to be linearizable; it merely was not
	// shown to be otherwise, and a caller relying on this check to prove
	// something should treat that as a gap rather than a pass.
	Inconclusive bool
}

func (r Result) String() string {
	if r.OK && r.Inconclusive {
		return "no violation found, but the search was abandoned on at least one key"
	}
	if r.OK {
		return "linearizable"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "NOT LINEARIZABLE on key %q: %s\n", r.Key, r.Reason)
	for _, op := range r.Ops {
		fmt.Fprintf(&b, "  %s\n", op)
	}
	return b.String()
}

// CheckLinearizable decides whether a history could have been produced by a
// linearizable key-value store.
//
// The history is partitioned by key and each part checked independently, which
// is sound because linearizability is *compositional*: a history over
// independent objects is linearizable exactly when every per-object sub-history
// is. That property is what makes checking tractable at all — the search is
// exponential in the number of concurrent operations, so splitting a history of
// thousands of operations into per-key groups of a handful turns an intractable
// problem into an easy one.
//
// Compare-and-swap is single-key here, so no operation spans two keys and the
// partition is valid. Adding a multi-key transaction would break that and
// require a whole-history search.
func CheckLinearizable(h *History) Result {
	byKey := make(map[string][]Op)
	for _, op := range h.Ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	// Sorted so that a failure is reported against the same key on every run.
	sort.Strings(keys)

	inconclusive := false
	for _, key := range keys {
		ops := byKey[key]
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Invoke < ops[j].Invoke })
		ok, conclusive := linearizeKeyDetailed(ops)
		if !ok {
			return Result{
				Key:    key,
				Ops:    ops,
				Reason: "no sequential order of these operations respects their real-time intervals",
			}
		}
		if !conclusive {
			inconclusive = true
		}
	}
	return Result{OK: true, Inconclusive: inconclusive}
}

// The search below is Wing and Gong's: try each operation that could go next,
// apply it to the sequential model, recurse, and backtrack when the model
// rejects it. Two details make it finish on histories of hundreds of operations.
//
// The first is how "which operations have been placed" is represented — a front
// index below which everything is placed, plus a bitmask of stragglers placed
// out of order above it. A history that is merely sequential never grows the
// mask, so it costs one step per operation rather than exploring permutations
// its own timing already forbids.
//
// The second is that operations which never returned are held apart from the
// rest. They are the reason a naive version of this search stalls: an operation
// with no return time overlaps everything after it, so it can be ordered almost
// anywhere, and leaving it in the front-index scheme means the front can never
// advance past it. There are only ever a handful of them — one per client at
// most — so they get their own bitmask and are placed when convenient.

// maxWindow bounds how far past the earliest unplaced operation the search will
// look. A client keeps one request outstanding, so the real window is the number
// of concurrent clients; this is slack.
const maxWindow = 48

// maxPending bounds how many never-returned operations one key may have before
// the search declines to try. It is a per-client phenomenon, so exceeding this
// means the history is not what this checker was built for.
const maxPending = 12

// searchBudget caps the states explored per key. Reaching it means the search
// was abandoned, not that the history was proved good.
const searchBudget = 1 << 20

// memoKey identifies a search state. Two paths that placed the same operations
// and arrived at the same value are interchangeable from here on, which is what
// collapses an exponential tree into something that finishes.
type memoKey struct {
	front   int
	mask    uint64
	pending uint16
	value   string
	present bool
}

type linearizer struct {
	// bounded holds operations that returned, sorted by invocation.
	bounded []Op
	// pending holds operations that never returned, sorted by invocation. Such an
	// operation has no upper bound on when it took effect, so it never forces
	// another operation to wait.
	pending []Op
	// required is the subset of pending that must still be placed: an operation
	// with a known outcome happened, whatever else is true of it. One whose
	// outcome the client never learned may simply never have happened.
	required uint16

	dead   map[memoKey]struct{}
	budget int
	gaveUp bool
}

// search reports whether the operations still outstanding can be ordered
// consistently, starting from state.
func (l *linearizer) search(front int, mask uint64, pmask uint16, state keyState) bool {
	if front == len(l.bounded) && l.required&^pmask == 0 {
		return true
	}
	if l.budget <= 0 {
		// Out of budget. Returning true is the conservative answer: failing to
		// find an order is not the same as proving none exists, and reporting a
		// violation here would be an accusation the search cannot support.
		l.gaveUp = true
		return true
	}
	l.budget--

	// An operation past the window could be the only one placeable next, and the
	// mask cannot reach it. The front operation blocks everything beyond the
	// window unless it reaches past it, so that is the only case where the window
	// can lose an ordering.
	if last := front + maxWindow; last < len(l.bounded) {
		if l.bounded[front].Return >= l.bounded[last].Invoke {
			l.gaveUp = true
			return true
		}
	}

	key := memoKey{front: front, mask: mask, pending: pmask, value: state.value, present: state.present}
	if _, seen := l.dead[key]; seen {
		return false
	}

	for j := front; j < len(l.bounded) && j < front+maxWindow; j++ {
		if mask&(1<<uint(j-front)) != 0 {
			continue // already placed
		}
		if !l.boundedPlaceable(front, mask, j) {
			continue
		}
		nextFront, nextMask := advance(front, mask, j)
		if l.branch(l.bounded[j], state, nextFront, nextMask, pmask) {
			return true
		}
	}

	for q := range l.pending {
		if pmask&(1<<uint(q)) != 0 {
			continue
		}
		if !l.pendingPlaceable(front, mask, q) {
			continue
		}
		if l.branch(l.pending[q], state, front, mask, pmask|1<<uint(q)) {
			return true
		}
	}

	l.dead[key] = struct{}{}
	return false
}

// branch explores placing one operation next.
//
// An operation whose outcome the client never learned gets two branches, since
// it may equally never have taken effect. Forcing either possibility would
// invent violations that are not there — a timed-out write that did commit looks
// like a lost write, and one that did not looks like a phantom.
func (l *linearizer) branch(op Op, state keyState, front int, mask uint64, pmask uint16) bool {
	if next, ok := state.apply(op); ok {
		if l.search(front, mask, pmask, next) {
			return true
		}
	}
	if op.Unknown {
		if l.search(front, mask, pmask, state) {
			return true
		}
	}
	return false
}

// boundedPlaceable reports whether bounded operation j may be linearized next.
//
// It may not if some unplaced operation returned before j was invoked: real time
// then puts that operation strictly first, and no sequential order may
// contradict it. That single comparison is the whole difference between
// linearizability and sequential consistency.
func (l *linearizer) boundedPlaceable(front int, mask uint64, j int) bool {
	invoke := l.bounded[j].Invoke
	for k := front; k < j; k++ {
		if mask&(1<<uint(k-front)) != 0 {
			continue // placed
		}
		// Operations are sorted by invocation, so anything after j was invoked no
		// earlier and cannot have returned before it. Only what lies between front
		// and j can block, and pending operations never block at all.
		if l.bounded[k].Return < invoke {
			return false
		}
	}
	return true
}

// pendingPlaceable reports whether the never-returned operation q may go next.
func (l *linearizer) pendingPlaceable(front int, mask uint64, q int) bool {
	invoke := l.pending[q].Invoke
	for k := front; k < len(l.bounded); k++ {
		if l.bounded[k].Invoke >= invoke {
			break // and so is everything after it
		}
		if k-front < maxWindow && mask&(1<<uint(k-front)) != 0 {
			continue // placed
		}
		if l.bounded[k].Return < invoke {
			return false
		}
	}
	return true
}

// advance marks j placed and slides the front index over any run of placed
// operations now sitting at the bottom of the mask.
func advance(front int, mask uint64, j int) (int, uint64) {
	mask |= 1 << uint(j-front)
	for mask&1 == 1 {
		mask >>= 1
		front++
	}
	return front, mask
}

// linearizeKeyDetailed searches for a valid order of one key's operations,
// reporting both the answer and whether the search was exhaustive.
//
// The distinction matters. "No order exists" is a claim about the store and
// worth failing a test over; "the search gave up" is a claim about the checker,
// and must never be dressed up as the former.
func linearizeKeyDetailed(ops []Op) (ok, conclusive bool) {
	l := &linearizer{dead: make(map[memoKey]struct{}), budget: searchBudget}
	for _, op := range ops {
		if op.Return == 0 {
			l.pending = append(l.pending, op)
		} else {
			l.bounded = append(l.bounded, op)
		}
	}
	if len(l.pending) > maxPending {
		return true, false
	}
	sort.SliceStable(l.bounded, func(i, j int) bool { return l.bounded[i].Invoke < l.bounded[j].Invoke })
	sort.SliceStable(l.pending, func(i, j int) bool { return l.pending[i].Invoke < l.pending[j].Invoke })
	for q, op := range l.pending {
		if !op.Unknown {
			l.required |= 1 << uint(q)
		}
	}

	ok = l.search(0, 0, 0, keyState{})
	return ok, !l.gaveUp
}

// linearizeKey reports whether one key's operations admit a valid sequential
// order. An abandoned search answers yes, since it has proved nothing.
func linearizeKey(ops []Op) bool {
	ok, _ := linearizeKeyDetailed(ops)
	return ok
}
