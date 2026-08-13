package raft

import (
	"fmt"
	"testing"
)

// --- basic replication -----------------------------------------------------

func TestProposalReplicatesToEveryNode(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	c.electLeader()

	c.propose("a")
	c.propose("b")
	c.propose("c")
	c.tick(2)

	c.assertCommittedEverywhere("a", "b", "c")
	c.assertNoDivergence()
}

func TestProposalRejectedByFollower(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	for _, id := range c.ids {
		if id == l.node.ID() {
			continue
		}
		// A follower cannot accept writes: it does not know whether it holds the
		// latest committed state, and appending locally would fork the log.
		if _, err := c.peers[id].node.Propose([]byte("x")); err != ErrProposalDropped {
			t.Errorf("node %d Propose error = %v, want ErrProposalDropped", id, err)
		}
	}
}

func TestCommitRequiresMajority(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	l := c.electLeader()

	// Isolate the leader with a single follower: two of five is not a majority,
	// so nothing may commit no matter how long it runs.
	var companion NodeID
	var rest []NodeID
	for _, id := range c.ids {
		switch {
		case id == l.node.ID():
		case companion == 0:
			companion = id
		default:
			rest = append(rest, id)
		}
	}
	c.partition([]NodeID{l.node.ID(), companion}, rest)

	before := l.node.CommitIndex()
	if _, err := l.node.Propose([]byte("minority-write")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.run()

	if l.node.CommitIndex() != before {
		t.Errorf("commit index advanced from %d to %d on a two-of-five minority",
			before, l.node.CommitIndex())
	}
	if l.node.LastIndex() <= before {
		t.Error("the entry was never appended, so the test proved nothing")
	}
}

func TestLeaderCountsItselfOnlyOncePersisted(t *testing.T) {
	// A 3-node cluster commits with the leader plus one follower. If the leader
	// counted its own entry before the fsync, a crash at that moment would lose
	// an entry the cluster had already been told was committed.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()

	// Clear the election's own batch so only the proposal is in flight.
	n.Advance(n.Ready())

	idx, err := n.Propose([]byte("v"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got := n.progress[n.id].Match; got >= idx {
		t.Errorf("leader Match = %d before persisting entry %d; it must not count an unfsynced write", got, idx)
	}

	// One follower acknowledges. That is one of three — not a majority without
	// the leader itself.
	if err := n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.Term(), MatchIndex: idx}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.CommitIndex() >= idx {
		t.Errorf("commit index = %d with only one follower acknowledging", n.CommitIndex())
	}

	// The driver reports the fsync. Now the leader is part of the majority.
	n.Advance(n.Ready())
	if n.CommitIndex() != idx {
		t.Errorf("commit index = %d after persisting, want %d", n.CommitIndex(), idx)
	}
}

// --- Figure 8: no commitment of entries from earlier terms -----------------

func TestNoCommitOfEarlierTermEntryByReplicaCount(t *testing.T) {
	// The Figure 8 defect: an entry from a previous term that is now present on a
	// majority is still *not* safe to commit, because a future leader can
	// legitimately overwrite it. Committing it can un-commit an applied write.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))

	// Two entries inherited from term 1.
	n.log.append(ents(1, 1)...)
	n.log.stableTo(2)

	// This node wins an election in a much later term.
	n.becomeFollower(3, None)
	n.becomeCandidate() // term 4
	n.becomeLeader()    // appends a no-op at index 3, term 4
	n.Advance(n.Ready())

	// A majority now holds the inherited entries through index 2.
	for _, id := range []NodeID{2, 3} {
		if err := n.Step(Message{Type: MsgAppResp, From: id, To: 1, Term: n.Term(), MatchIndex: 2}); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	if n.CommitIndex() >= 2 {
		t.Fatalf("commit index = %d: an entry from term 1 was committed by replica count alone", n.CommitIndex())
	}

	// Once an entry of the leader's own term commits, the earlier entries are
	// committed transitively — and safely, because no future leader can now win
	// without them.
	for _, id := range []NodeID{2, 3} {
		if err := n.Step(Message{Type: MsgAppResp, From: id, To: 1, Term: n.Term(), MatchIndex: 3}); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	if n.CommitIndex() != 3 {
		t.Errorf("commit index = %d, want 3 once the current-term no-op is on a majority", n.CommitIndex())
	}
}

func TestReadIndexRefusedBeforeCurrentTermCommit(t *testing.T) {
	// A leader that has not yet committed an entry of its own term cannot bound a
	// linearizable read, because its commit index may still regress. It must
	// refuse rather than serve from an inherited index.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.log.append(ents(1, 1)...)
	n.log.stableTo(2)
	n.log.commitTo(2)
	n.becomeFollower(3, None)
	n.becomeCandidate()
	n.becomeLeader()

	n.ReadIndex([]byte("r1"))

	rd := n.Ready()
	for _, rs := range rd.ReadStates {
		if string(rs.ReadCtx) == "r1" && rs.Index != 0 {
			t.Errorf("read was served at index %d before the current term committed", rs.Index)
		}
	}
}

// --- catching up a lagging follower ---------------------------------------

func TestOfflineFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	var victim NodeID
	for _, id := range c.ids {
		if id != l.node.ID() {
			victim = id
			break
		}
	}

	c.crash(victim)
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("v%02d", i))
	}
	c.tick(2)

	// The remaining majority commits without the absent node.
	if l.node.CommitIndex() < 20 {
		t.Fatalf("commit index = %d, expected the majority to make progress", l.node.CommitIndex())
	}

	c.restart(victim)
	c.tick(20)

	c.assertNoDivergence()
	got := c.peers[victim].appliedData()
	if len(got) != 20 {
		t.Fatalf("recovered node applied %d commands, want 20", len(got))
	}
	for i, v := range got {
		if want := fmt.Sprintf("v%02d", i); v != want {
			t.Fatalf("recovered node applied %q at position %d, want %q", v, i, want)
		}
	}
}

func TestEntriesAreBatchedWithinLimit(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3}, withMaxEntriesPerMessage(4))
	c.electLeader()

	var victim NodeID
	l := c.leader()
	for _, id := range c.ids {
		if id != l.node.ID() {
			victim = id
			break
		}
	}

	// Build a backlog while one node is away, then watch it catch up. A single
	// unbounded message could exceed the transport's frame limit, so the backlog
	// must be delivered in bounded pieces.
	c.crash(victim)
	for i := 0; i < 30; i++ {
		c.propose(fmt.Sprintf("v%02d", i))
	}
	c.restart(victim)

	maxSeen := 0
	for i := 0; i < 40 && len(c.peers[victim].applied) == 0; i++ {
		c.tick(1)
	}
	// Inspect every append the leader produces during catch-up.
	c.resetCounters()
	c.tick(20)
	for _, m := range c.queue {
		if len(m.Entries) > maxSeen {
			maxSeen = len(m.Entries)
		}
	}
	if maxSeen > 4 {
		t.Errorf("an AppendEntries carried %d entries, above the limit of 4", maxSeen)
	}
	c.assertNoDivergence()
	if got := len(c.peers[victim].appliedData()); got != 30 {
		t.Errorf("recovered node applied %d commands, want 30", got)
	}
}

// --- log repair after divergence ------------------------------------------

func TestDivergentSuffixIsRepaired(t *testing.T) {
	// A partitioned leader accumulates uncommitted entries that the rest of the
	// cluster never saw. When it rejoins, that suffix must be discarded and
	// replaced by the real leader's log.
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	old := c.electLeader()
	oldID := old.node.ID()

	c.propose("shared")

	// Isolate the leader. It keeps appending locally, but nothing commits.
	c.isolate(oldID)
	for i := 0; i < 10; i++ {
		if _, err := old.node.Propose([]byte(fmt.Sprintf("orphan%02d", i))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	c.run()
	orphanIndex := old.node.LastIndex()

	// The majority elects a replacement and commits its own writes.
	c.tick(40)
	var fresh *peer
	for _, id := range c.ids {
		if id != oldID && c.peers[id].node.Role() == Leader {
			fresh = c.peers[id]
		}
	}
	if fresh == nil {
		t.Fatal("the majority never elected a replacement leader")
	}
	for i := 0; i < 3; i++ {
		if _, err := fresh.node.Propose([]byte(fmt.Sprintf("real%02d", i))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		c.run()
	}

	// Heal. The old leader must step down and have its orphaned suffix replaced.
	c.heal()
	c.tick(40)

	if old.node.Role() == Leader {
		t.Error("the returning leader did not step down")
	}
	for _, v := range old.appliedData() {
		if len(v) >= 6 && v[:6] == "orphan" {
			t.Errorf("SAFETY VIOLATION: the returning node applied orphaned entry %q", v)
		}
	}
	if old.node.LastIndex() > orphanIndex {
		// Its log should now match the real leader's, not retain the orphans.
		t.Logf("old leader last index %d (was %d)", old.node.LastIndex(), orphanIndex)
	}
	c.assertNoDivergence()

	// Every live node must converge on the real leader's history.
	want := fresh.appliedData()
	for _, id := range c.ids {
		got := c.peers[id].appliedData()
		if len(got) != len(want) {
			t.Errorf("node %d applied %v, want %v", id, got, want)
		}
	}
}

func TestRepairSkipsWholeConflictingTermPerRoundTrip(t *testing.T) {
	// Log repair must cost round trips proportional to the number of conflicting
	// *terms*, not conflicting entries. The naive one-index-per-rejection walk
	// would need hundreds of round trips here.
	l := newTestNode(t, 1, NewConfig(1, 2, 3))

	// The leader's log: index 1 at term 1, then 200 entries at term 5.
	terms := []Term{1}
	for i := 0; i < 200; i++ {
		terms = append(terms, 5)
	}
	l.log.append(ents(terms...)...)
	l.log.stableTo(l.log.lastIndex())
	l.becomeFollower(5, None)
	l.becomeCandidate() // term 6
	l.becomeLeader()
	l.Advance(l.Ready())

	// The follower shares only index 1, then has 200 entries from a different
	// term that must all be discarded.
	fTerms := []Term{1}
	for i := 0; i < 200; i++ {
		fTerms = append(fTerms, 3)
	}
	f := newTestNode(t, 2, NewConfig(1, 2, 3))
	f.log.append(ents(fTerms...)...)
	f.log.stableTo(f.log.lastIndex())
	f.becomeFollower(6, 1)

	// Pump messages between the two nodes by hand, counting how many
	// leader→follower appends it takes to reconcile the logs.
	roundTrips := 0
	for iter := 0; iter < 500; iter++ {
		if l.progress[2].Match >= l.log.lastIndex() {
			break
		}
		// If the leader has nothing queued, prod it the way a heartbeat interval
		// would, so a lost probe cannot end the exchange early.
		if len(l.msgs) == 0 {
			l.progress[2].ProbeSent = false
			l.sendAppend(2)
		}

		rd := l.Ready()
		l.Advance(rd)
		for _, m := range rd.Messages {
			if m.To != 2 || m.Type != MsgAppReq {
				continue
			}
			roundTrips++
			if err := f.Step(m); err != nil {
				t.Fatalf("follower Step: %v", err)
			}
		}

		frd := f.Ready()
		f.Advance(frd)
		for _, m := range frd.Messages {
			if m.To == 1 {
				if err := l.Step(m); err != nil {
					t.Fatalf("leader Step: %v", err)
				}
			}
		}
	}

	if l.progress[2].Match < l.log.lastIndex() {
		t.Fatalf("repair did not complete: Match = %d, leader last index = %d",
			l.progress[2].Match, l.log.lastIndex())
	}
	// Two conflicting terms means a handful of round trips. Anything approaching
	// 200 would mean the hint is not being used.
	if roundTrips > 15 {
		t.Errorf("repair took %d round trips for 200 conflicting entries; the divergence hint is not working", roundTrips)
	}
	t.Logf("repaired 200 conflicting entries in %d round trips", roundTrips)
}

func TestFollowerHintPointsAtDivergence(t *testing.T) {
	f := newTestNode(t, 2, NewConfig(1, 2, 3))
	f.log.append(ents(1, 1, 2, 2, 2)...)
	f.becomeFollower(5, 1)

	// The leader probes at index 5 claiming term 4; the follower has term 2
	// there. Its hint should point at the start of its own term-2 run so the
	// leader skips the whole run at once.
	if err := f.Step(Message{
		Type: MsgAppReq, From: 1, To: 2, Term: 5,
		PrevLogIndex: 5, PrevLogTerm: 4,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, f, MsgAppResp)
	if !resp.Reject {
		t.Fatal("the follower accepted a mismatched append")
	}
	if resp.MatchIndex != 5 {
		t.Errorf("rejected index = %d, want 5", resp.MatchIndex)
	}
	// The follower reports the highest position at which its own term is no
	// greater than the probed term. Here its term at index 5 is already 2, below
	// the probed 4, so index 5 is the answer. The large skip comes from the
	// leader's half of the walk, which this hint enables.
	if resp.HintIndex != 5 || resp.HintTerm != 2 {
		t.Errorf("hint = (%d, %d), want (5, 2)", resp.HintIndex, resp.HintTerm)
	}
}

func TestLeaderSkipsItsOwnConflictingTermOnHint(t *testing.T) {
	// The leader's half of the two-sided walk. Given a follower whose log
	// diverges at term 2, the leader must skip past every one of its own entries
	// with a higher term in a single step rather than probing them one by one.
	l := newTestNode(t, 1, NewConfig(1, 2, 3))
	terms := []Term{1, 2}
	for i := 0; i < 50; i++ {
		terms = append(terms, 7)
	}
	l.log.append(ents(terms...)...)
	l.log.stableTo(l.log.lastIndex())
	l.becomeFollower(7, None)
	l.becomeCandidate() // term 8
	l.becomeLeader()
	l.Advance(l.Ready())

	last := l.log.lastIndex()
	pr := l.progress[2]
	pr.Next = last + 1

	// The follower says: at index 52 my term is 2.
	if err := l.Step(Message{
		Type: MsgAppResp, From: 2, To: 1, Term: l.Term(),
		Reject: true, MatchIndex: last, HintIndex: 52, HintTerm: 2,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	// Every leader entry from index 3 up is term 7 or 8, so none can match a
	// follower at term 2. Next must land at or below index 3 in one step.
	if pr.Next > 3 {
		t.Errorf("Next = %d after one rejection, want <= 3: the leader did not skip its own conflicting term",
			pr.Next)
	}
}

func TestFollowerBelowCommitIndexAnswersWithCommit(t *testing.T) {
	f := newTestNode(t, 2, NewConfig(1, 2, 3))
	f.log.append(ents(1, 1, 1, 1, 1)...)
	f.log.commitTo(5)
	f.becomeFollower(5, 1)

	// A probe below the commit index cannot conflict — that prefix is settled.
	// Answering with the commit index lets the leader jump straight there instead
	// of walking back one index at a time.
	if err := f.Step(Message{
		Type: MsgAppReq, From: 1, To: 2, Term: 5,
		PrevLogIndex: 2, PrevLogTerm: 9, // deliberately wrong term
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, f, MsgAppResp)
	if resp.Reject {
		t.Error("a probe below the commit index was rejected instead of answered with the commit index")
	}
	if resp.MatchIndex != 5 {
		t.Errorf("MatchIndex = %d, want 5 (the follower's commit index)", resp.MatchIndex)
	}
}

// --- duplicate and reordered messages -------------------------------------

func TestDuplicateAppendIsIdempotent(t *testing.T) {
	f := newTestNode(t, 2, NewConfig(1, 2, 3))
	f.becomeFollower(5, 1)

	m := Message{
		Type: MsgAppReq, From: 1, To: 2, Term: 5,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: ents(5, 5, 5),
		Commit:  3,
	}
	for i := 0; i < 4; i++ {
		if err := f.Step(m); err != nil {
			t.Fatalf("Step %d: %v", i, err)
		}
	}
	if got := f.LastIndex(); got != 3 {
		t.Errorf("last index = %d after four identical appends, want 3", got)
	}
	if got := f.CommitIndex(); got != 3 {
		t.Errorf("commit index = %d, want 3", got)
	}
}

func TestStaleAcknowledgementDoesNotRegressMatch(t *testing.T) {
	l := newTestNode(t, 1, NewConfig(1, 2, 3))
	l.becomeCandidate()
	l.becomeLeader()
	l.Advance(l.Ready())
	for i := 0; i < 5; i++ {
		if _, err := l.Propose([]byte("v")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	l.Advance(l.Ready())

	if err := l.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: l.Term(), MatchIndex: 5}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := l.progress[2].Match; got != 5 {
		t.Fatalf("Match = %d, want 5", got)
	}

	// A duplicated or reordered older acknowledgement must not lower Match: the
	// commit index is computed from it, and lowering it could un-commit an entry.
	if err := l.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: l.Term(), MatchIndex: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := l.progress[2].Match; got != 5 {
		t.Errorf("Match regressed to %d after a stale acknowledgement, want 5", got)
	}
}

func TestStaleRejectionIsIgnoredWhileStreaming(t *testing.T) {
	l := newTestNode(t, 1, NewConfig(1, 2, 3))
	l.becomeCandidate()
	l.becomeLeader()
	l.Advance(l.Ready())
	for i := 0; i < 5; i++ {
		if _, err := l.Propose([]byte("v")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	l.Advance(l.Ready())

	// Establish a stream: node 2 has acknowledged through index 4.
	if err := l.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: l.Term(), MatchIndex: 4}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if l.progress[2].State != stateReplicate {
		t.Fatalf("state = %s, want replicate", l.progress[2].State)
	}

	// A rejection at an index already acknowledged is stale by definition, and
	// acting on it would knock a healthy stream back into probing.
	if err := l.Step(Message{
		Type: MsgAppResp, From: 2, To: 1, Term: l.Term(),
		Reject: true, MatchIndex: 3, HintIndex: 1,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if l.progress[2].State != stateReplicate {
		t.Errorf("a stale rejection dropped the peer to %s", l.progress[2].State)
	}
	if l.progress[2].Match != 4 {
		t.Errorf("Match = %d after a stale rejection, want 4", l.progress[2].Match)
	}
}

// --- committed entries survive failover -----------------------------------

func TestCommittedWritesSurviveRepeatedFailover(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	c.electLeader()

	var want []string
	for round := 0; round < 4; round++ {
		for i := 0; i < 3; i++ {
			v := fmt.Sprintf("r%dv%d", round, i)
			c.propose(v)
			want = append(want, v)
		}
		c.tick(2)

		// Depose the current leader and let the cluster recover.
		old := c.leader().node.ID()
		c.crash(old)
		c.tick(60)
		c.restart(old)
		c.tick(30)
		c.assertNoDivergence()
	}

	c.tick(30)
	// Every command acknowledged as committed must still be present, in order,
	// on every node — across four leader changes.
	for _, id := range c.ids {
		got := c.peers[id].appliedData()
		if len(got) < len(want) {
			t.Errorf("node %d applied %d commands, want at least %d", id, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("node %d position %d = %q, want %q", id, i, got[i], want[i])
				break
			}
		}
	}
}

func TestRestartRecoversFromPersistedStateAlone(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	c.electLeader()
	for i := 0; i < 8; i++ {
		c.propose(fmt.Sprintf("v%d", i))
	}
	c.tick(2)

	// Restart every node at once. Nothing survives except what the driver was
	// told to persist, so any entry the core failed to report through Ready is
	// now permanently lost.
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	c.tick(60)

	c.assertNoDivergence()
	for _, id := range c.ids {
		got := c.peers[id].appliedData()
		if len(got) < 8 {
			t.Fatalf("node %d recovered %d commands, want 8: %v", id, len(got), got)
		}
		for i := 0; i < 8; i++ {
			if want := fmt.Sprintf("v%d", i); got[i] != want {
				t.Fatalf("node %d position %d = %q, want %q", id, i, got[i], want)
			}
		}
	}
}

// --- linearizable reads ---------------------------------------------------

func TestReadIndexReflectsAcknowledgedWrite(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	c.propose("written")
	c.tick(2)
	committed := l.node.CommitIndex()

	l.reads = nil
	l.node.ReadIndex([]byte("after-write"))
	c.run()

	if len(l.reads) == 0 {
		t.Fatal("no read state was delivered")
	}
	rs := l.reads[len(l.reads)-1]
	if rs.Index < committed {
		t.Errorf("read index = %d, below the committed index %d: the read could miss an acknowledged write",
			rs.Index, committed)
	}
}

func TestReadIndexNeedsQuorumConfirmation(t *testing.T) {
	// A leader cut off from the cluster must not release a read. Serving it would
	// return state that a new leader has already moved past.
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	c.propose("v")
	c.tick(2)

	c.isolate(l.node.ID())
	l.reads = nil
	l.node.ReadIndex([]byte("isolated"))
	c.run()

	for _, rs := range l.reads {
		if string(rs.ReadCtx) == "isolated" && rs.Index != 0 {
			t.Errorf("an isolated leader released a read at index %d without quorum confirmation", rs.Index)
		}
	}
}

func TestPendingReadsRefusedWhenLeadershipLost(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	c.propose("v")
	c.tick(2)

	// Ask for a read, then cut the leader off so it can never be confirmed. When
	// the leader steps down, the read must be refused rather than left hanging.
	c.isolate(l.node.ID())
	l.reads = nil
	l.node.ReadIndex([]byte("doomed"))
	c.tick(40)

	if l.node.Role() == Leader {
		t.Fatal("the isolated leader never stepped down")
	}
	found := false
	for _, rs := range l.reads {
		if string(rs.ReadCtx) == "doomed" {
			found = true
			if rs.Index != 0 {
				t.Errorf("read released at index %d by a deposed leader", rs.Index)
			}
		}
	}
	if !found {
		t.Error("the pending read was neither refused nor answered; a client would hang")
	}
	if len(l.node.readOnly) != 0 {
		t.Errorf("%d reads left queued after losing leadership", len(l.node.readOnly))
	}
}

func TestFollowerForwardsReadToLeader(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	c.propose("v")
	c.tick(2)

	var follower *peer
	for _, id := range c.ids {
		if id != l.node.ID() {
			follower = c.peers[id]
			break
		}
	}
	follower.reads = nil
	follower.node.ReadIndex([]byte("via-follower"))
	c.run()

	if len(follower.reads) == 0 {
		t.Fatal("the follower never received a read state; the forward was lost")
	}
	rs := follower.reads[len(follower.reads)-1]
	if rs.Index == 0 {
		t.Error("the forwarded read was refused, want it answered by the leader")
	}
	if rs.Index > l.node.CommitIndex() {
		t.Errorf("read index %d exceeds the leader's commit index %d", rs.Index, l.node.CommitIndex())
	}
}

// --- larger randomised runs ------------------------------------------------

func TestReplicationUnderRepeatedPartitions(t *testing.T) {
	// A longer scenario mixing partitions, healing and continuous writes. The
	// assertion is the invariant, not a specific outcome: no two nodes may ever
	// disagree about what sits at a given log index.
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	c.electLeader()

	writes := 0
	for round := 0; round < 6; round++ {
		switch round % 3 {
		case 0:
			c.heal()
		case 1:
			c.partition([]NodeID{1, 2, 3}, []NodeID{4, 5})
		case 2:
			c.partition([]NodeID{1, 2}, []NodeID{3, 4, 5})
		}
		c.tick(30)

		// Write through whichever node currently leads, if any.
		if ls := c.leaders(); len(ls) > 0 {
			for _, id := range ls {
				p := c.peers[id]
				if _, err := p.node.Propose([]byte(fmt.Sprintf("r%dn%d", round, id))); err == nil {
					writes++
				}
			}
			c.run()
		}
		c.assertNoDivergence()
	}

	if writes == 0 {
		t.Fatal("no write was ever accepted, so the test proved nothing")
	}
	c.heal()
	c.tick(80)
	c.assertNoDivergence()

	// After healing, every node must agree on the committed prefix.
	l := c.leader()
	want := l.appliedData()
	for _, id := range c.ids {
		got := c.peers[id].appliedData()
		n := min(len(got), len(want))
		for i := 0; i < n; i++ {
			if got[i] != want[i] {
				t.Fatalf("node %d diverges from the leader at position %d: %q vs %q", id, i, got[i], want[i])
			}
		}
	}
}
