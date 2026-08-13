package raft

import "fmt"

// raftLog is the replicated log: an ordered, append-only sequence of entries
// with a compaction boundary at the front.
//
// Storage model: entries after the snapshot boundary are held in memory, and
// durability is provided by the caller's write-ahead log rather than by reading
// back from disk. This trades memory for a large simplification — every log
// query is an in-memory slice index, so the consensus core never blocks on I/O
// and never has to handle a read error mid-decision. The cost is bounded by the
// snapshot threshold: compaction trims the prefix, so steady-state footprint is
// (threshold * mean entry size), not the lifetime of the cluster.
//
// Index arithmetic is the usual source of Raft bugs, so it is confined here.
// The invariant, maintained by every method below:
//
//	entries[i] has Index == snapIndex + 1 + i
//
// meaning entries[0] is the first entry *after* the boundary, and an empty
// entries slice denotes a log whose contents are entirely captured by the
// snapshot.
type raftLog struct {
	// snapIndex and snapTerm describe the last entry folded into the snapshot.
	// They act as a virtual entry preceding entries[0], which keeps the
	// AppendEntries consistency check working across the boundary: a leader
	// probing at exactly snapIndex can still be answered.
	snapIndex Index
	snapTerm  Term

	entries []Entry

	// committed is the highest index known to be replicated to a majority.
	committed Index
	// applied is the highest index handed to the state machine. Entries in
	// (applied, committed] are pending application.
	applied Index
	// stable is the highest index the caller has confirmed durable. Entries in
	// (stable, lastIndex()] are what Ready reports as needing persistence.
	stable Index
}

// newLog returns an empty log positioned at the given boundary. A fresh cluster
// starts at (0, 0), so the first appended entry lands at index 1.
func newLog(snapIndex Index, snapTerm Term) *raftLog {
	return &raftLog{
		snapIndex: snapIndex,
		snapTerm:  snapTerm,
		committed: snapIndex,
		applied:   snapIndex,
		stable:    snapIndex,
	}
}

// firstIndex is the lowest index still retrievable from the log.
func (l *raftLog) firstIndex() Index { return l.snapIndex + 1 }

// lastIndex is the highest index present, or snapIndex for an empty log.
func (l *raftLog) lastIndex() Index { return l.snapIndex + Index(len(l.entries)) }

// term returns the term of the entry at i.
//
// Callers must distinguish the two failures: ErrCompacted means the entry
// existed but has been folded into a snapshot (the leader should send that
// snapshot), while ErrUnavailable means it was never there (a protocol bug, or
// a stale message).
func (l *raftLog) term(i Index) (Term, error) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, nil
	case i < l.snapIndex:
		return 0, ErrCompacted
	case i > l.lastIndex():
		return 0, ErrUnavailable
	default:
		return l.entries[i-l.snapIndex-1].Term, nil
	}
}

// lastTerm returns the term of the final entry. The log always has a term —
// falling back to snapTerm when empty — so election comparisons need no
// special case for a freshly restored node.
func (l *raftLog) lastTerm() Term {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// entry returns the entry at i, or false if it is outside the retained range.
func (l *raftLog) entry(i Index) (Entry, bool) {
	if i <= l.snapIndex || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[i-l.snapIndex-1], true
}

// slice returns entries in [lo, hi), capped at maxEntries when maxEntries > 0.
// The result is a fresh slice: handing out a view into l.entries would let a
// later truncation mutate entries a caller is still holding, or in-flight
// messages already queued for the transport.
func (l *raftLog) slice(lo, hi Index, maxEntries int) ([]Entry, error) {
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid slice %d > %d", lo, hi)
	}
	if lo <= l.snapIndex {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	if maxEntries > 0 && int(hi-lo) > maxEntries {
		hi = lo + Index(maxEntries)
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1])
	return out, nil
}

// matchTerm reports whether the log holds an entry at index i with term t. This
// is the AppendEntries consistency check (§5.3): agreement at one position
// implies, inductively, agreement on the entire prefix.
func (l *raftLog) matchTerm(i Index, t Term) bool {
	got, err := l.term(i)
	if err != nil {
		return false
	}
	return got == t
}

// isUpToDate implements the §5.4.1 election restriction: a vote may only be
// granted to a candidate whose log is at least as current as the voter's,
// compared by last term first and length only as a tiebreak.
//
// This is what makes leader election sufficient for safety. Because a candidate
// needs a majority of votes, and every voter holds a log at least as current as
// the candidate's, a candidate missing a committed entry cannot win: any
// majority necessarily includes a node that has that entry and would refuse.
func (l *raftLog) isUpToDate(lastIdx Index, lastTerm Term) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= l.lastIndex()
}

// append adds entries to the end of the log, renumbering nothing: callers are
// responsible for handing over entries whose indexes continue the log.
func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	if want := l.lastIndex() + 1; ents[0].Index != want {
		panic(fmt.Sprintf("raft: append gap: first entry index %d, expected %d", ents[0].Index, want))
	}
	l.entries = append(l.entries, ents...)
}

// maybeAppend performs the follower side of AppendEntries. It verifies the
// consistency check at (prevIndex, prevTerm), reconciles any divergence, and
// advances the commit index. It returns the index of the last new entry and
// whether the check passed.
func (l *raftLog) maybeAppend(prevIndex Index, prevTerm Term, leaderCommit Index, ents []Entry) (Index, bool) {
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}

	lastNew := prevIndex + Index(len(ents))
	conflict := l.findConflict(ents)
	switch {
	case conflict == 0:
		// Every entry is already present with a matching term. This happens
		// routinely on retransmission and must be idempotent — truncating here
		// could discard entries the leader has since committed.
	case conflict <= l.committed:
		// A committed entry can never be overwritten: it is durable on a
		// majority and may already have been served to a client. Reaching this
		// state means the protocol was violated, so fail loudly rather than
		// silently corrupting the log.
		panic(fmt.Sprintf("raft: entry %d conflicts with committed index %d", conflict, l.committed))
	default:
		// Divergence: discard the conflicting suffix and adopt the leader's.
		// The leader's log is authoritative for its term, and by the election
		// restriction it cannot be missing anything committed.
		l.truncateFrom(conflict)
		l.entries = append(l.entries, ents[conflict-prevIndex-1:]...)
	}

	// The commit index may not outrun what this node actually holds: the leader
	// may be reporting a commit covering entries carried by a later message.
	l.commitTo(min(leaderCommit, lastNew))
	return lastNew, true
}

// findConflict returns the index of the first entry in ents that disagrees with
// the log, or 0 if all of them agree. An entry past the end of the log counts
// as a conflict, since it is new and must be appended.
func (l *raftLog) findConflict(ents []Entry) Index {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// findConflictByTerm walks back from index to the first entry whose term is at
// most term, returning that index and its term.
//
// It backs the rejection hint that turns log repair from O(entries) round trips
// into O(conflicting terms): rather than the leader decrementing nextIndex by
// one per rejection, the follower reports the start of the whole term it
// disagrees on, letting the leader skip it in a single step. That matters after
// a partition heals with thousands of divergent entries.
func (l *raftLog) findConflictByTerm(index Index, term Term) (Index, Term) {
	if index > l.lastIndex() {
		index = l.lastIndex()
	}
	for index > l.snapIndex {
		t, err := l.term(index)
		if err != nil || t <= term {
			break
		}
		index--
	}
	t, err := l.term(index)
	if err != nil {
		return index, 0
	}
	return index, t
}

// truncateFrom discards the entry at i and everything after it.
func (l *raftLog) truncateFrom(i Index) {
	if i <= l.snapIndex || i > l.lastIndex() {
		return
	}
	l.entries = l.entries[:i-l.snapIndex-1]
	// Durability regresses with the log: anything the caller had persisted at
	// or beyond i is no longer part of this log, so it must be re-reported by
	// Ready and rewritten. Failing to rewind here is how a truncated suffix
	// survives a crash and resurrects entries the cluster has discarded.
	if l.stable >= i {
		l.stable = i - 1
	}
}

// commitTo advances the commit index. It only ever moves forward; a stale or
// duplicated message must not walk it backwards.
func (l *raftLog) commitTo(i Index) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit index %d exceeds last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

// appliedTo records that the state machine has consumed through i.
func (l *raftLog) appliedTo(i Index) {
	if i > l.applied {
		l.applied = i
	}
}

// stableTo records that the caller has durably stored through i.
func (l *raftLog) stableTo(i Index) {
	if i > l.stable && i <= l.lastIndex() {
		l.stable = i
	}
}

// unstableEntries returns the entries not yet reported durable, which Ready
// hands to the caller for persistence.
func (l *raftLog) unstableEntries() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	ents, err := l.slice(l.stable+1, l.lastIndex()+1, 0)
	if err != nil {
		panic(fmt.Sprintf("raft: unreachable: unstable entries unavailable: %v", err))
	}
	return ents
}

// nextCommitted returns the committed entries awaiting application.
func (l *raftLog) nextCommitted(maxEntries int) []Entry {
	if l.applied >= l.committed {
		return nil
	}
	ents, err := l.slice(l.applied+1, l.committed+1, maxEntries)
	if err != nil {
		panic(fmt.Sprintf("raft: unreachable: committed entries unavailable: %v", err))
	}
	return ents
}

// hasPendingApply reports whether the state machine is behind the commit index.
func (l *raftLog) hasPendingApply() bool { return l.committed > l.applied }

// compact folds the prefix through index i into the snapshot boundary,
// releasing those entries. Only applied entries may be compacted: discarding an
// entry the state machine has not yet consumed would lose it permanently.
func (l *raftLog) compact(i Index) error {
	if i <= l.snapIndex {
		return ErrCompacted
	}
	if i > l.applied {
		return fmt.Errorf("raft: cannot compact to %d beyond applied index %d", i, l.applied)
	}
	t, err := l.term(i)
	if err != nil {
		return err
	}
	l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	l.snapIndex = i
	l.snapTerm = t
	if l.stable < i {
		l.stable = i
	}
	return nil
}

// restore resets the log to a snapshot received from a leader, discarding local
// entries entirely.
//
// Wholesale replacement is safe, and necessary: the snapshot is committed state
// from a leader whose log — by the election restriction — contains everything
// this node could have had committed. Any local entry not covered by it was
// uncommitted and is the leader's to overwrite.
func (l *raftLog) restore(snap Snapshot) error {
	if snap.Index <= l.snapIndex {
		return ErrSnapshotOutOfDate
	}
	// A snapshot the log already covers with a matching term carries no new
	// information; keep the richer local log instead of throwing it away.
	if l.matchTerm(snap.Index, snap.Term) {
		l.commitTo(snap.Index)
		return ErrSnapshotOutOfDate
	}
	l.entries = nil
	l.snapIndex = snap.Index
	l.snapTerm = snap.Term
	l.committed = snap.Index
	l.applied = snap.Index
	l.stable = snap.Index
	return nil
}
