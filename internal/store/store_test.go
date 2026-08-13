package store

import (
	"bytes"
	"fmt"
	"testing"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// entryFor wraps a command as the log entry the state machine would receive.
func entryFor(t testing.TB, index raft.Index, cmd *kvv1.Command) raft.Entry {
	t.Helper()
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	return raft.Entry{Term: 1, Index: index, Type: raft.EntryNormal, Data: data}
}

func put(key, value string) *kvv1.Command {
	return &kvv1.Command{Op: kvv1.OpType_OP_TYPE_PUT, Key: []byte(key), Value: []byte(value)}
}

func del(key string) *kvv1.Command {
	return &kvv1.Command{Op: kvv1.OpType_OP_TYPE_DELETE, Key: []byte(key)}
}

func cas(key, expected, next string) *kvv1.Command {
	return &kvv1.Command{
		Op:            kvv1.OpType_OP_TYPE_CAS,
		Key:           []byte(key),
		ExpectedValue: []byte(expected),
		Value:         []byte(next),
	}
}

func casAbsent(key, next string) *kvv1.Command {
	return &kvv1.Command{
		Op:           kvv1.OpType_OP_TYPE_CAS,
		Key:          []byte(key),
		ExpectAbsent: true,
		Value:        []byte(next),
	}
}

func withSession(cmd *kvv1.Command, clientID, seq uint64) *kvv1.Command {
	cmd.Header = &kvv1.RequestHeader{ClientId: clientID, Sequence: seq}
	return cmd
}

func apply(t testing.TB, s *Store, index raft.Index, cmd *kvv1.Command) *kvv1.CommandResult {
	t.Helper()
	res, err := s.Apply(entryFor(t, index, cmd))
	if err != nil {
		t.Fatalf("Apply at index %d: %v", index, err)
	}
	return res
}

func mustGet(t testing.TB, s *Store, key string) string {
	t.Helper()
	v, ok := s.Get([]byte(key))
	if !ok {
		t.Fatalf("key %q is absent", key)
	}
	return string(v)
}

// --- basic operations ------------------------------------------------------

func TestPutGetDelete(t *testing.T) {
	s := New()

	apply(t, s, 1, put("a", "1"))
	if got := mustGet(t, s, "a"); got != "1" {
		t.Errorf("a = %q, want 1", got)
	}

	// Overwriting is a plain replacement.
	apply(t, s, 2, put("a", "2"))
	if got := mustGet(t, s, "a"); got != "2" {
		t.Errorf("a = %q, want 2", got)
	}

	res := apply(t, s, 3, del("a"))
	if !res.GetExisted() {
		t.Error("Existed = false when deleting a present key")
	}
	if _, ok := s.Get([]byte("a")); ok {
		t.Error("key is still present after deletion")
	}

	// Deleting an absent key succeeds but reports that nothing was there.
	res = apply(t, s, 4, del("a"))
	if res.GetExisted() {
		t.Error("Existed = true when deleting an absent key")
	}
}

func TestGetReturnsIndependentCopy(t *testing.T) {
	s := New()
	apply(t, s, 1, put("k", "value"))

	got, _ := s.Get([]byte("k"))
	got[0] = 'X'

	if again := mustGet(t, s, "k"); again != "value" {
		t.Errorf("k = %q after the caller mutated a read result, want %q", again, "value")
	}
}

func TestNonStateMachineEntriesHaveNoEffect(t *testing.T) {
	s := New()
	apply(t, s, 1, put("a", "1"))

	// A no-op and a configuration change are interpreted by the consensus layer,
	// not the state machine. They must advance the applied index and change
	// nothing else.
	for i, e := range []raft.Entry{
		{Term: 1, Index: 2, Type: raft.EntryNoOp},
		{Term: 1, Index: 3, Type: raft.EntryConfChange, Data: []byte("membership")},
	} {
		if _, err := s.Apply(e); err != nil {
			t.Fatalf("Apply(%d): %v", i, err)
		}
	}

	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
	if s.AppliedIndex() != 3 {
		t.Errorf("AppliedIndex() = %d, want 3", s.AppliedIndex())
	}
}

func TestUndecodableCommandIsReported(t *testing.T) {
	// Replicas that disagree about whether an entry had an effect diverge, so
	// applying is not a place to be tolerant.
	s := New()
	_, err := s.Apply(raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte{0xff, 0xff, 0xff}})
	if err == nil {
		t.Error("Apply accepted an undecodable command")
	}
}

func TestUnknownOpcodeIsIgnoredDeterministically(t *testing.T) {
	// What a rolling upgrade looks like from the old build's side: an opcode it
	// does not recognise. Every replica running this build must agree the entry
	// had no effect.
	s := New()
	apply(t, s, 1, &kvv1.Command{Op: kvv1.OpType(99), Key: []byte("k"), Value: []byte("v")})
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
	if s.AppliedIndex() != 1 {
		t.Errorf("AppliedIndex() = %d, want 1", s.AppliedIndex())
	}
}

// --- compare-and-swap ------------------------------------------------------

func TestCompareAndSwap(t *testing.T) {
	s := New()
	apply(t, s, 1, put("k", "old"))

	res := apply(t, s, 2, cas("k", "old", "new"))
	if !res.GetSwapped() {
		t.Fatal("a matching compare-and-swap did not swap")
	}
	if got := mustGet(t, s, "k"); got != "new" {
		t.Errorf("k = %q, want new", got)
	}

	// The expectation no longer holds, so the swap must fail and report what is
	// actually there — no follow-up read needed.
	res = apply(t, s, 3, cas("k", "old", "newer"))
	if res.GetSwapped() {
		t.Error("a mismatched compare-and-swap swapped")
	}
	if !res.GetFound() || string(res.GetCurrentValue()) != "new" {
		t.Errorf("failed swap reported (found %v, %q), want (true, %q)",
			res.GetFound(), res.GetCurrentValue(), "new")
	}
	if got := mustGet(t, s, "k"); got != "new" {
		t.Errorf("k = %q after a failed swap, want it unchanged at new", got)
	}
}

func TestCompareAndSwapCreateIfAbsent(t *testing.T) {
	s := New()

	res := apply(t, s, 1, casAbsent("fresh", "value"))
	if !res.GetSwapped() {
		t.Fatal("create-if-absent failed on a missing key")
	}
	if got := mustGet(t, s, "fresh"); got != "value" {
		t.Errorf("fresh = %q, want value", got)
	}

	// A second attempt must fail: the key now exists.
	res = apply(t, s, 2, casAbsent("fresh", "other"))
	if res.GetSwapped() {
		t.Error("create-if-absent succeeded on an existing key")
	}
	if !res.GetFound() || string(res.GetCurrentValue()) != "value" {
		t.Errorf("reported (found %v, %q), want (true, %q)",
			res.GetFound(), res.GetCurrentValue(), "value")
	}
}

func TestCompareAndSwapOnAbsentKeyWithExpectedValueFails(t *testing.T) {
	s := New()
	res := apply(t, s, 1, cas("missing", "something", "new"))
	if res.GetSwapped() {
		t.Error("swapped against a key that does not exist")
	}
	if res.GetFound() {
		t.Error("Found = true for an absent key")
	}
	if s.Len() != 0 {
		t.Error("a failed swap created the key")
	}
}

func TestCompareAndSwapSerialisesRacingClients(t *testing.T) {
	// The property that makes the store useful for coordination: two clients
	// racing to claim the same key through compare-and-swap cannot both win,
	// because the log gives their commands an order.
	s := New()

	first := apply(t, s, 1, casAbsent("lock", "client-a"))
	second := apply(t, s, 2, casAbsent("lock", "client-b"))

	if !first.GetSwapped() {
		t.Error("the first claim failed")
	}
	if second.GetSwapped() {
		t.Error("both clients acquired the same lock")
	}
	if got := mustGet(t, s, "lock"); got != "client-a" {
		t.Errorf("lock = %q, want client-a", got)
	}
}

// --- exactly-once semantics ------------------------------------------------

func TestDuplicateCommandAppliedOnce(t *testing.T) {
	// Raft guarantees at-least-once application. A client whose response was lost
	// retries, and the same command reaches the log twice. Without the session
	// table the second copy would be applied again.
	s := New()
	apply(t, s, 1, put("counter", "1"))

	cmd := withSession(casAbsent("claim", "mine"), 7, 1)
	first := apply(t, s, 2, cmd)
	if !first.GetSwapped() {
		t.Fatal("the first attempt failed")
	}

	// The retry arrives as a separate log entry with the same client and sequence.
	retry := apply(t, s, 3, withSession(casAbsent("claim", "mine"), 7, 1))
	if !retry.GetSwapped() {
		t.Error("the retry returned failure; a replay must return the original result")
	}
	if got := mustGet(t, s, "claim"); got != "mine" {
		t.Errorf("claim = %q, want mine", got)
	}
}

func TestDuplicateDeleteReturnsOriginalResult(t *testing.T) {
	// The clearest case: a delete applied twice would report "did not exist" the
	// second time, telling the client the opposite of what happened.
	s := New()
	apply(t, s, 1, put("k", "v"))

	first := apply(t, s, 2, withSession(del("k"), 3, 1))
	if !first.GetExisted() {
		t.Fatal("the first delete reported the key as absent")
	}
	retry := apply(t, s, 3, withSession(del("k"), 3, 1))
	if !retry.GetExisted() {
		t.Error("the replayed delete reported Existed = false, contradicting the original answer")
	}
}

func TestNewSequenceIsAppliedNormally(t *testing.T) {
	s := New()
	apply(t, s, 1, withSession(put("a", "1"), 5, 1))
	apply(t, s, 2, withSession(put("a", "2"), 5, 2))

	if got := mustGet(t, s, "a"); got != "2" {
		t.Errorf("a = %q, want 2: a new sequence number is a new request", got)
	}
}

func TestSessionsAreIndependentPerClient(t *testing.T) {
	s := New()
	// Same sequence number from two different clients must not collide.
	first := apply(t, s, 1, withSession(casAbsent("k", "client-1"), 1, 1))
	second := apply(t, s, 2, withSession(casAbsent("k", "client-2"), 2, 1))

	if !first.GetSwapped() {
		t.Error("client 1's claim failed")
	}
	if second.GetSwapped() {
		t.Error("client 2's claim succeeded; it should have lost the race")
	}
}

func TestCommandsWithoutClientIDAreNotDeduplicated(t *testing.T) {
	// A caller that supplies no identity has opted out of the guarantee, and must
	// not accidentally share a session slot with everyone else who did the same.
	s := New()
	apply(t, s, 1, put("n", "1"))
	apply(t, s, 2, put("n", "2"))
	if got := mustGet(t, s, "n"); got != "2" {
		t.Errorf("n = %q, want 2", got)
	}
}

// --- snapshots -------------------------------------------------------------

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	for i := 0; i < 50; i++ {
		apply(t, s, raft.Index(i+1), put(fmt.Sprintf("key%02d", i), fmt.Sprintf("value%02d", i)))
	}
	apply(t, s, 51, withSession(del("key00"), 9, 4))

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(data, s.AppliedIndex()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.Len() != s.Len() {
		t.Errorf("restored %d keys, want %d", restored.Len(), s.Len())
	}
	if restored.AppliedIndex() != s.AppliedIndex() {
		t.Errorf("restored applied index %d, want %d", restored.AppliedIndex(), s.AppliedIndex())
	}
	for i := 1; i < 50; i++ {
		key := fmt.Sprintf("key%02d", i)
		if got := mustGet(t, restored, key); got != fmt.Sprintf("value%02d", i) {
			t.Errorf("%s = %q after restore", key, got)
		}
	}
	if _, ok := restored.Get([]byte("key00")); ok {
		t.Error("a deleted key came back through the snapshot")
	}
}

func TestSnapshotCarriesSessionTable(t *testing.T) {
	// A failover that forgot which requests it had served would re-apply them,
	// which is exactly what the session table exists to prevent. So the sessions
	// must travel inside the snapshot.
	s := New()
	apply(t, s, 1, put("k", "v"))
	first := apply(t, s, 2, withSession(del("k"), 11, 1))
	if !first.GetExisted() {
		t.Fatal("the original delete reported the key as absent")
	}

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := New()
	if err := restored.Restore(data, s.AppliedIndex()); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// The same request replayed against the restored state machine must return the
	// original answer, not be applied afresh.
	retry := apply(t, restored, 3, withSession(del("k"), 11, 1))
	if !retry.GetExisted() {
		t.Error("the restored state machine lost the session and re-applied the delete")
	}
}

func TestSnapshotIsByteIdenticalForIdenticalState(t *testing.T) {
	// Two replicas that applied the same commands must produce the same bytes.
	// Without a fixed ordering, Go's randomised map iteration would make every
	// snapshot differ and any checksum over one meaningless.
	build := func() *Store {
		s := New()
		for i := 0; i < 40; i++ {
			apply(t, s, raft.Index(i+1), put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)))
		}
		apply(t, s, 41, withSession(put("z", "1"), 3, 1))
		apply(t, s, 42, withSession(put("y", "2"), 8, 1))
		return s
	}

	first, err := build().Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for i := 0; i < 20; i++ {
		next, err := build().Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if !bytes.Equal(first, next) {
			t.Fatalf("snapshot %d differs from the first for identical state", i)
		}
	}
}

func TestRestoreReplacesRatherThanMerges(t *testing.T) {
	// A snapshot is a complete description of committed state. Anything held
	// locally that it does not mention was uncommitted or superseded, so merging
	// would resurrect it.
	s := New()
	apply(t, s, 1, put("keep", "yes"))
	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	other := New()
	apply(t, other, 1, put("stale", "should-be-gone"))
	if err := other.Restore(data, 1); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := other.Get([]byte("stale")); ok {
		t.Error("Restore merged instead of replacing: a stale key survived")
	}
	if got := mustGet(t, other, "keep"); got != "yes" {
		t.Errorf("keep = %q, want yes", got)
	}
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	s := New()
	if err := s.Restore([]byte{0xff, 0xfe, 0xfd}, 1); err == nil {
		t.Error("Restore accepted a corrupt snapshot")
	}
}

func TestEmptySnapshotRestoresEmptyStore(t *testing.T) {
	s := New()
	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := New()
	apply(t, restored, 1, put("x", "y"))
	if err := restored.Restore(data, 0); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Len() != 0 {
		t.Errorf("Len() = %d after restoring an empty snapshot, want 0", restored.Len())
	}
}

// --- determinism across replicas ------------------------------------------

func TestSameCommandSequenceYieldsSameState(t *testing.T) {
	// The core requirement of a replicated state machine: same commands, same
	// order, same result — on every replica, every time.
	commands := []*kvv1.Command{
		put("a", "1"), put("b", "2"), cas("a", "1", "3"),
		del("b"), casAbsent("c", "4"), cas("c", "wrong", "5"),
		put("d", "6"), del("nonexistent"),
	}

	var reference []byte
	for replica := 0; replica < 5; replica++ {
		s := New()
		for i, cmd := range commands {
			// A fresh copy per replica, since Apply must not depend on shared state.
			apply(t, s, raft.Index(i+1), cmd)
		}
		data, err := s.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if replica == 0 {
			reference = data
			continue
		}
		if !bytes.Equal(reference, data) {
			t.Fatalf("replica %d reached different state from the same command sequence", replica)
		}
	}
}
