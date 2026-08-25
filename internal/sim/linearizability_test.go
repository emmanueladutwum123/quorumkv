package sim

import "testing"

// A linearizability checker that always answers "yes" would make every chaos test
// pass while verifying nothing. So the checker is tested in both directions: it
// must accept histories that are genuinely possible, and reject ones that are
// not — including the specific violations a real consensus bug would produce.

func op(client int, kind OpKind, key string, invoke, ret int64) Op {
	return Op{ClientID: client, Kind: kind, Key: key, Invoke: invoke, Return: ret}
}

func putOp(client int, key, value string, invoke, ret int64) Op {
	o := op(client, OpPut, key, invoke, ret)
	o.Value = value
	return o
}

func getOp(client int, key, gotValue string, found bool, invoke, ret int64) Op {
	o := op(client, OpGet, key, invoke, ret)
	o.GotValue = gotValue
	o.GotFound = found
	return o
}

func casOp(client int, key, expected, next string, swapped bool, invoke, ret int64) Op {
	o := op(client, OpCAS, key, invoke, ret)
	o.Expected = expected
	o.Value = next
	o.Swapped = swapped
	return o
}

func createOp(client int, key, next string, swapped bool, invoke, ret int64) Op {
	o := op(client, OpCAS, key, invoke, ret)
	o.ExpectAbsent = true
	o.Value = next
	o.Swapped = swapped
	return o
}

func delOp(client int, key string, existed bool, invoke, ret int64) Op {
	o := op(client, OpDelete, key, invoke, ret)
	o.Existed = existed
	return o
}

func check(t testing.TB, ops ...Op) Result {
	t.Helper()
	h := &History{}
	for _, o := range ops {
		h.Add(o)
	}
	return CheckLinearizable(h)
}

// --- histories that must be accepted -------------------------------------

func TestEmptyHistoryIsLinearizable(t *testing.T) {
	if res := check(t); !res.OK {
		t.Errorf("empty history rejected: %s", res)
	}
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	res := check(t,
		putOp(1, "a", "1", 1, 2),
		getOp(1, "a", "1", true, 3, 4),
		putOp(1, "a", "2", 5, 6),
		getOp(1, "a", "2", true, 7, 8),
		delOp(1, "a", true, 9, 10),
		getOp(1, "a", "", false, 11, 12),
	)
	if !res.OK {
		t.Errorf("a plainly sequential history was rejected: %s", res)
	}
}

func TestConcurrentWritesEitherOrderIsLinearizable(t *testing.T) {
	// Two overlapping writes and a read that sees one of them. Both orders are
	// possible, so the history is linearizable.
	res := check(t,
		putOp(1, "k", "a", 1, 10),
		putOp(2, "k", "b", 2, 11),
		getOp(3, "k", "b", true, 12, 13),
	)
	if !res.OK {
		t.Errorf("rejected a valid concurrent history: %s", res)
	}

	res = check(t,
		putOp(1, "k", "a", 1, 10),
		putOp(2, "k", "b", 2, 11),
		getOp(3, "k", "a", true, 12, 13),
	)
	if !res.OK {
		t.Errorf("rejected the other valid ordering: %s", res)
	}
}

func TestReadConcurrentWithWriteMaySeeEitherValue(t *testing.T) {
	// A read overlapping a write may be ordered before or after it.
	for _, seen := range []struct {
		value string
		found bool
	}{{"old", true}, {"new", true}} {
		res := check(t,
			putOp(1, "k", "old", 1, 2),
			putOp(1, "k", "new", 5, 15),
			getOp(2, "k", seen.value, seen.found, 6, 14),
		)
		if !res.OK {
			t.Errorf("a read seeing %q during an overlapping write was rejected: %s", seen.value, res)
		}
	}
}

func TestIndependentKeysDoNotInterfere(t *testing.T) {
	res := check(t,
		putOp(1, "a", "1", 1, 2),
		putOp(2, "b", "2", 1, 2),
		getOp(1, "a", "1", true, 3, 4),
		getOp(2, "b", "2", true, 3, 4),
	)
	if !res.OK {
		t.Errorf("independent keys were treated as interfering: %s", res)
	}
}

func TestUnknownOutcomeMayOrMayNotHaveHappened(t *testing.T) {
	// A write that timed out may have committed or not, so a later read must be
	// accepted either way.
	timedOut := putOp(1, "k", "maybe", 1, 10)
	timedOut.Unknown = true

	if res := check(t, timedOut, getOp(2, "k", "maybe", true, 11, 12)); !res.OK {
		t.Errorf("a read seeing a timed-out write was rejected: %s", res)
	}
	if res := check(t, timedOut, getOp(2, "k", "", false, 11, 12)); !res.OK {
		t.Errorf("a read NOT seeing a timed-out write was rejected: %s", res)
	}
}

func TestOperationThatNeverReturnedImposesNoUpperBound(t *testing.T) {
	// An operation still in flight at the end of the history can be placed
	// anywhere, including after everything else.
	pending := putOp(1, "k", "late", 1, 0)
	pending.Unknown = true
	res := check(t, pending, getOp(2, "k", "", false, 2, 3))
	if !res.OK {
		t.Errorf("a still-pending operation was forced into the history: %s", res)
	}
}

// --- histories that must be rejected -------------------------------------

func TestStaleReadIsRejected(t *testing.T) {
	// The violation a partitioned leader would produce: a write completes, then a
	// strictly later read returns the previous value. No sequential order allows
	// it, because the read begins after the write returned.
	res := check(t,
		putOp(1, "k", "old", 1, 2),
		putOp(1, "k", "new", 3, 4),
		getOp(2, "k", "old", true, 5, 6),
	)
	if res.OK {
		t.Fatal("a stale read was accepted; the checker cannot detect the very bug it exists for")
	}
	if res.Key != "k" {
		t.Errorf("blamed key %q, want k", res.Key)
	}
}

func TestLostWriteIsRejected(t *testing.T) {
	// A write acknowledged and then absent from a later read: committed data lost.
	res := check(t,
		putOp(1, "k", "v", 1, 2),
		getOp(2, "k", "", false, 3, 4),
	)
	if res.OK {
		t.Error("a lost acknowledged write was accepted")
	}
}

func TestTwoSuccessfulCreatesOfTheSameKeyAreRejected(t *testing.T) {
	// The split-brain signature: two clients both told they created the same key.
	// Only one position in any sequential order can find it absent.
	res := check(t,
		createOp(1, "lock", "client-a", true, 1, 10),
		createOp(2, "lock", "client-b", true, 2, 11),
	)
	if res.OK {
		t.Error("two successful creates of one key were accepted; that is split-brain")
	}
}

func TestCompareAndSwapReportingWrongOutcomeIsRejected(t *testing.T) {
	// The key holds "a", so a swap expecting "b" must fail. Claiming success is
	// impossible.
	res := check(t,
		putOp(1, "k", "a", 1, 2),
		casOp(2, "k", "b", "c", true, 3, 4),
	)
	if res.OK {
		t.Error("a compare-and-swap that succeeded against the wrong value was accepted")
	}
}

func TestDeleteReportingWrongExistenceIsRejected(t *testing.T) {
	res := check(t,
		putOp(1, "k", "v", 1, 2),
		delOp(2, "k", false, 3, 4), // claims the key was absent
	)
	if res.OK {
		t.Error("a delete that misreported existence was accepted")
	}
}

func TestReadSeeingAFutureWriteIsRejected(t *testing.T) {
	// A read that returns a value written strictly after it returned.
	res := check(t,
		getOp(1, "k", "future", true, 1, 2),
		putOp(2, "k", "future", 3, 4),
	)
	if res.OK {
		t.Error("a read that observed a write from its own future was accepted")
	}
}

func TestNonOverlappingReadsMustAgreeWithWriteOrder(t *testing.T) {
	// Two sequential reads that disagree, with no write between them.
	res := check(t,
		putOp(1, "k", "v", 1, 2),
		getOp(2, "k", "v", true, 3, 4),
		getOp(2, "k", "other", true, 5, 6),
	)
	if res.OK {
		t.Error("a read returned a value nobody ever wrote and it was accepted")
	}
}

func TestValueNeverWrittenIsRejected(t *testing.T) {
	res := check(t, getOp(1, "k", "invented", true, 1, 2))
	if res.OK {
		t.Error("a read of a value that was never written was accepted")
	}
}

func TestViolationOnOneKeyIsCaughtAmongManyValidKeys(t *testing.T) {
	// The bad key must be found even when surrounded by well-behaved traffic, and
	// the report must name it.
	ops := []Op{
		putOp(1, "good1", "x", 1, 2),
		getOp(1, "good1", "x", true, 3, 4),
		putOp(2, "good2", "y", 1, 2),
		getOp(2, "good2", "y", true, 3, 4),
		putOp(3, "bad", "old", 1, 2),
		putOp(3, "bad", "new", 3, 4),
		getOp(4, "bad", "old", true, 5, 6), // stale
	}
	res := check(t, ops...)
	if res.OK {
		t.Fatal("the violating key was not detected")
	}
	if res.Key != "bad" {
		t.Errorf("blamed key %q, want \"bad\"", res.Key)
	}
	if len(res.Ops) == 0 {
		t.Error("the report contains no operations to diagnose from")
	}
}

// --- the model itself ------------------------------------------------------

func TestKeyStateApply(t *testing.T) {
	tests := []struct {
		name    string
		state   keyState
		op      Op
		wantOK  bool
		wantVal string
		wantHas bool
	}{
		{"put on empty", keyState{}, putOp(1, "k", "v", 1, 2), true, "v", true},
		{"get absent correctly", keyState{}, getOp(1, "k", "", false, 1, 2), true, "", false},
		{"get absent wrongly", keyState{}, getOp(1, "k", "v", true, 1, 2), false, "", false},
		{"get present correctly", keyState{"v", true}, getOp(1, "k", "v", true, 1, 2), true, "v", true},
		{"get wrong value", keyState{"v", true}, getOp(1, "k", "other", true, 1, 2), false, "v", true},
		{"delete present", keyState{"v", true}, delOp(1, "k", true, 1, 2), true, "", false},
		{"delete absent", keyState{}, delOp(1, "k", false, 1, 2), true, "", false},
		{"cas match", keyState{"a", true}, casOp(1, "k", "a", "b", true, 1, 2), true, "b", true},
		{"cas mismatch", keyState{"a", true}, casOp(1, "k", "z", "b", false, 1, 2), true, "a", true},
		{"create on absent", keyState{}, createOp(1, "k", "v", true, 1, 2), true, "v", true},
		{"create on present", keyState{"a", true}, createOp(1, "k", "v", false, 1, 2), true, "a", true},
		{"create claims success on present", keyState{"a", true}, createOp(1, "k", "v", true, 1, 2), false, "a", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, ok := tt.state.apply(tt.op)
			if ok != tt.wantOK {
				t.Fatalf("apply ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if next.value != tt.wantVal || next.present != tt.wantHas {
				t.Errorf("state = %+v, want value %q present %v", next, tt.wantVal, tt.wantHas)
			}
		})
	}
}

func TestLargeSequentialHistoryIsHandled(t *testing.T) {
	// More operations on one key than fit in the bitmask search. Because they do
	// not overlap, splitting at the sequential boundaries is exact.
	var ops []Op
	var tick int64 = 1
	for i := 0; i < 300; i++ {
		value := "v" + string(rune('a'+i%26))
		ops = append(ops, putOp(1, "k", value, tick, tick+1))
		tick += 2
		ops = append(ops, getOp(1, "k", value, true, tick, tick+1))
		tick += 2
	}
	if res := check(t, ops...); !res.OK {
		t.Errorf("a long sequential history was rejected: %s", res)
	}
}

func TestLargeSequentialHistoryStillCatchesViolation(t *testing.T) {
	// The split must not become a way to miss violations.
	var ops []Op
	var tick int64 = 1
	for i := 0; i < 200; i++ {
		ops = append(ops, putOp(1, "k", "good", tick, tick+1))
		tick += 2
		ops = append(ops, getOp(1, "k", "good", true, tick, tick+1))
		tick += 2
	}
	ops = append(ops, getOp(2, "k", "wrong", true, tick, tick+1))
	if res := check(t, ops...); res.OK {
		t.Error("a violation at the end of a long history was missed")
	}
}

func TestResultStringIsInformative(t *testing.T) {
	res := check(t,
		putOp(1, "k", "old", 1, 2),
		putOp(1, "k", "new", 3, 4),
		getOp(2, "k", "old", true, 5, 6),
	)
	if res.OK {
		t.Fatal("expected a violation")
	}
	msg := res.String()
	for _, want := range []string{"NOT LINEARIZABLE", "k", "get"} {
		if !containsStr(msg, want) {
			t.Errorf("failure message does not mention %q:\n%s", want, msg)
		}
	}
	if ok := check(t, putOp(1, "k", "v", 1, 2)); ok.String() != "linearizable" {
		t.Errorf("success message = %q", ok.String())
	}
}

func containsStr(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// --- the search itself -----------------------------------------------------

func TestHeavilyConcurrentHistoryIsAccepted(t *testing.T) {
	// Every operation overlaps every other, so the search cannot rely on real
	// time to order anything and must actually explore permutations. The history
	// is valid: the reads see values that were written.
	ops := []Op{
		putOp(1, "k", "a", 1, 100),
		putOp(2, "k", "b", 2, 100),
		putOp(3, "k", "c", 3, 100),
		getOp(4, "k", "a", true, 4, 100),
		getOp(5, "k", "c", true, 5, 100),
		delOp(6, "k", true, 6, 100),
	}
	if res := check(t, ops...); !res.OK {
		t.Errorf("a valid fully-concurrent history was rejected: %s", res)
	}
}

func TestConcurrencyDoesNotExcuseAnImpossibleValue(t *testing.T) {
	// Same shape as above, but one read returns a value nobody wrote. No
	// permutation can explain it, so overlap must not become a blanket excuse.
	ops := []Op{
		putOp(1, "k", "a", 1, 100),
		putOp(2, "k", "b", 2, 100),
		putOp(3, "k", "c", 3, 100),
		getOp(4, "k", "z", true, 4, 100),
	}
	if res := check(t, ops...); res.OK {
		t.Error("a read of a value nobody wrote was excused by concurrency")
	}
}

func TestNeverReturnedOperationCanBePlacedArbitrarilyLate(t *testing.T) {
	// An operation still in flight overlaps everything after it, so it may be
	// ordered right at the end -- here, immediately before the read that sees it.
	// Getting this wrong is what makes a checker abandon histories that are in
	// fact perfectly ordinary.
	pending := putOp(1, "k", "pending", 1, 0)
	pending.Unknown = true
	ops := []Op{pending}
	var tick int64 = 2
	for i := 0; i < maxWindow*2; i++ {
		ops = append(ops, putOp(2, "k", "v", tick, tick+1))
		tick += 2
	}
	ops = append(ops, getOp(3, "k", "pending", true, tick, tick+1))

	res := check(t, ops...)
	if !res.OK {
		t.Fatalf("a late-placed pending operation was called a violation: %s", res)
	}
	if res.Inconclusive {
		t.Error("the search gave up on a history it should decide")
	}
}

func TestNeverReturnedOperationDoesNotExcuseAViolation(t *testing.T) {
	// The same shape, but the read returns a value no operation ever wrote. A
	// pending operation must not become a wildcard that explains anything.
	pending := putOp(1, "k", "pending", 1, 0)
	pending.Unknown = true
	ops := []Op{pending}
	var tick int64 = 2
	for i := 0; i < maxWindow*2; i++ {
		ops = append(ops, putOp(2, "k", "v", tick, tick+1))
		tick += 2
	}
	ops = append(ops, getOp(3, "k", "never-written", true, tick, tick+1))

	if res := check(t, ops...); res.OK {
		t.Error("an in-flight operation was used to excuse a value nobody wrote")
	}
}

func TestAbandonedSearchIsReportedRatherThanPassedOff(t *testing.T) {
	// Past the limit on never-returned operations the search declines to try.
	// That must be reported, because a history nobody decided verified nothing --
	// silently returning "linearizable" would hollow out every scenario that
	// relies on this check.
	var ops []Op
	for i := 0; i < maxPending+1; i++ {
		p := putOp(i, "k", "v", int64(i+1), 0)
		p.Unknown = true
		ops = append(ops, p)
	}
	res := check(t, ops...)
	if !res.OK {
		t.Fatalf("a history the search declined to decide was reported as a violation: %s", res)
	}
	if !res.Inconclusive {
		t.Error("the search was abandoned but the result does not say so")
	}
	if res.String() == "linearizable" {
		t.Errorf("an abandoned search prints as a clean pass: %q", res.String())
	}
}

func TestOrdinaryHistoryIsDecidedConclusively(t *testing.T) {
	// The guard above must not fire on everyday traffic, or it would hollow out
	// every scenario in sim_test.go without failing anything.
	var ops []Op
	var tick int64 = 1
	for i := 0; i < 200; i++ {
		ops = append(ops, putOp(1, "k", "v", tick, tick+2))
		ops = append(ops, getOp(2, "k", "v", true, tick+1, tick+3))
		tick += 4
	}
	res := check(t, ops...)
	if !res.OK {
		t.Fatalf("rejected: %s", res)
	}
	if res.Inconclusive {
		t.Error("the search gave up on an ordinary overlapping history")
	}
}
