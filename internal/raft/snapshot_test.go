package raft

import (
	"fmt"
	"testing"
)

func TestCompactionReleasesLogPrefix(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("v%02d", i))
	}
	c.tick(2)

	before := l.node.SnapshotIndex()
	applied := l.node.AppliedIndex()
	c.snapshotAndCompact(l.node.ID())

	if l.node.SnapshotIndex() != applied {
		t.Errorf("snapshot index = %d, want %d", l.node.SnapshotIndex(), applied)
	}
	if l.node.SnapshotIndex() <= before {
		t.Error("compaction did not advance the boundary")
	}
	// The last index must be unchanged: compaction reclaims the prefix, it does
	// not shorten the log.
	if l.node.LastIndex() < applied {
		t.Errorf("last index = %d, below the compaction boundary %d", l.node.LastIndex(), applied)
	}
	// The cluster must keep working across the boundary.
	c.propose("after-compaction")
	c.tick(2)
	c.assertNoDivergence()
}

func TestCompactionBeyondAppliedIsRefused(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	c.propose("a")
	c.tick(2)

	// Discarding an entry the state machine has not consumed would lose it
	// permanently: nothing else holds a copy of its effects.
	if err := l.node.Compact(l.node.AppliedIndex() + 5); err == nil {
		t.Error("Compact succeeded past the applied index")
	}
}

func TestLaggingFollowerCaughtUpBySnapshot(t *testing.T) {
	// A follower that falls behind the leader's compaction boundary cannot be
	// repaired with log entries: the entries it needs no longer exist. It must be
	// sent a snapshot instead.
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
	for i := 0; i < 30; i++ {
		c.propose(fmt.Sprintf("v%02d", i))
	}
	c.tick(2)

	// Compact past everything the absent node holds.
	snap := c.snapshotAndCompact(l.node.ID())
	if snap.Index < 20 {
		t.Fatalf("snapshot boundary %d is too low to force a snapshot transfer", snap.Index)
	}

	c.resetCounters()
	c.restart(victim)
	c.tick(40)

	if c.sent[MsgSnapReq] == 0 {
		t.Fatal("no InstallSnapshot was sent to a follower behind the compaction boundary")
	}
	v := c.peers[victim]
	if v.node.SnapshotIndex() < snap.Index {
		t.Errorf("follower snapshot index = %d, want at least %d", v.node.SnapshotIndex(), snap.Index)
	}
	// The follower's state machine must reflect the full history, reconstructed
	// from the snapshot rather than replayed from the log.
	got := v.appliedData()
	if len(got) != 30 {
		t.Fatalf("follower applied %d commands after the snapshot, want 30", len(got))
	}
	for i, v := range got {
		if want := fmt.Sprintf("v%02d", i); v != want {
			t.Fatalf("follower position %d = %q, want %q", i, v, want)
		}
	}
	c.assertNoDivergence()
}

func TestClusterContinuesAfterSnapshotTransfer(t *testing.T) {
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
	for i := 0; i < 25; i++ {
		c.propose(fmt.Sprintf("old%02d", i))
	}
	c.tick(2)
	c.snapshotAndCompact(l.node.ID())
	c.restart(victim)
	c.tick(40)

	// Normal replication must resume through log entries once the follower is
	// caught up, not repeat the snapshot.
	c.resetCounters()
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprintf("new%d", i))
	}
	c.tick(5)

	if c.sent[MsgSnapReq] != 0 {
		t.Errorf("%d further snapshots were sent after the follower caught up", c.sent[MsgSnapReq])
	}
	c.assertNoDivergence()
	got := c.peers[victim].appliedData()
	if len(got) != 30 {
		t.Fatalf("follower applied %d commands, want 30", len(got))
	}
	if got[29] != "new4" {
		t.Errorf("last applied = %q, want new4", got[29])
	}
}

func TestSnapshotRestoreAdoptsMembership(t *testing.T) {
	// A restoring node has no log left to replay membership changes from, so the
	// configuration must come from the snapshot itself.
	n := newTestNode(t, 4, NewConfig(4))

	snap := Snapshot{
		Index: 10,
		Term:  3,
		Conf: Config{
			Voters:   [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()},
			Learners: NewNodeSet(4),
		},
		Data: []byte("state"),
	}
	if err := n.Step(Message{Type: MsgSnapReq, From: 1, To: 4, Term: 3, Snapshot: &snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	cfg := n.Membership()
	if !cfg.IsVoter(1) || !cfg.IsVoter(2) || !cfg.IsVoter(3) {
		t.Errorf("membership after restore = %s, want voters 1-3", cfg)
	}
	if !cfg.IsLearner(4) {
		t.Errorf("membership after restore = %s, want node 4 to be a learner", cfg)
	}
	if n.CommitIndex() != 10 || n.AppliedIndex() != 10 {
		t.Errorf("commit/applied = %d/%d, want 10/10", n.CommitIndex(), n.AppliedIndex())
	}
	// The driver must be told to persist and apply it, rather than the core doing
	// so behind the caller's back.
	rd := n.Ready()
	if rd.Snapshot == nil {
		t.Error("Ready did not surface the restored snapshot for persistence")
	}
	resp := lastMessage(t, n, MsgSnapResp)
	if resp.Reject || resp.MatchIndex != 10 {
		t.Errorf("response = (reject %v, match %d), want (false, 10)", resp.Reject, resp.MatchIndex)
	}
}

func TestStaleSnapshotIsRefused(t *testing.T) {
	n := newTestNode(t, 2, NewConfig(1, 2, 3))
	n.log.append(ents(1, 1, 1, 1, 1)...)
	n.log.commitTo(5)
	n.becomeFollower(2, 1)

	// A snapshot the node has already surpassed must not move its state machine
	// backwards.
	snap := Snapshot{Index: 3, Term: 1, Conf: NewConfig(1, 2, 3)}
	if err := n.Step(Message{Type: MsgSnapReq, From: 1, To: 2, Term: 2, Snapshot: &snap}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.CommitIndex() != 5 {
		t.Errorf("commit index = %d, want 5: a stale snapshot must not regress it", n.CommitIndex())
	}
	if n.LastIndex() != 5 {
		t.Errorf("last index = %d, want 5: the richer local log must be kept", n.LastIndex())
	}
	resp := lastMessage(t, n, MsgSnapResp)
	if resp.MatchIndex != 5 {
		t.Errorf("response match index = %d, want 5 so the leader resumes from the right place", resp.MatchIndex)
	}
}

func TestMissingSnapshotPayloadIsRejected(t *testing.T) {
	n := newTestNode(t, 2, NewConfig(1, 2, 3))
	n.becomeFollower(2, 1)
	if err := n.Step(Message{Type: MsgSnapReq, From: 1, To: 2, Term: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, n, MsgSnapResp)
	if !resp.Reject {
		t.Error("an InstallSnapshot with no payload was accepted")
	}
}

func TestLeaderWithoutSnapshotProviderPausesInsteadOfSpinning(t *testing.T) {
	// With no way to produce a snapshot, a follower behind the boundary cannot be
	// repaired. Pausing is the honest outcome: it keeps the leader from retrying a
	// probe that can never succeed, and the stall is visible in peer status.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.log.append(ents(1, 1, 1, 1, 1)...)
	n.log.stableTo(5)
	n.log.commitTo(5)
	n.log.appliedTo(5)
	if err := n.log.compact(4); err != nil {
		t.Fatalf("compact: %v", err)
	}
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	pr := n.progress[2]
	pr.Match = 0
	pr.becomeProbe() // Next = 1, below the compaction boundary
	n.msgs = nil

	n.sendAppend(2)

	if pr.State != stateSnapshot {
		t.Errorf("progress state = %s, want snapshot", pr.State)
	}
	for _, m := range n.msgs {
		if m.Type == MsgSnapReq {
			t.Error("a snapshot was sent despite no provider being configured")
		}
	}
	if !pr.isPaused() {
		t.Error("replication to an unrepairable follower was not paused")
	}
}

func TestSnapshotProviderFailureLeavesPeerRepairable(t *testing.T) {
	// If taking a snapshot fails transiently, the leader must not record a
	// snapshot as in flight — that would pause replication to the peer with no
	// message to resume it.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.opts.Snapshots = failingSnapshots{}
	n.log.append(ents(1, 1, 1, 1, 1)...)
	n.log.stableTo(5)
	n.log.commitTo(5)
	n.log.appliedTo(5)
	if err := n.log.compact(4); err != nil {
		t.Fatalf("compact: %v", err)
	}
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	pr := n.progress[2]
	pr.Match = 0
	pr.becomeProbe()
	n.msgs = nil

	n.sendAppend(2)

	if pr.State == stateSnapshot {
		t.Error("a failed snapshot attempt left the peer marked as receiving one")
	}
}

type failingSnapshots struct{}

func (failingSnapshots) Snapshot() (Snapshot, error) {
	return Snapshot{}, fmt.Errorf("snapshot unavailable")
}

func TestRestartFromSnapshotPlusLog(t *testing.T) {
	// The full recovery path: restore a snapshot, replay the log that follows it,
	// and end up with the same state the node had before it crashed.
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	leaderID := l.node.ID()

	for i := 0; i < 15; i++ {
		c.propose(fmt.Sprintf("pre%02d", i))
	}
	c.tick(2)
	c.snapshotAndCompact(leaderID)

	// More writes after the boundary, which must be recovered from the log rather
	// than the snapshot.
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprintf("post%d", i))
	}
	c.tick(2)

	beforeApplied := len(l.appliedData())
	c.crash(leaderID)
	c.restart(leaderID)
	c.tick(40)

	got := c.peers[leaderID].appliedData()
	if len(got) != beforeApplied {
		t.Fatalf("recovered %d commands, want %d", len(got), beforeApplied)
	}
	for i := 0; i < 15; i++ {
		if want := fmt.Sprintf("pre%02d", i); got[i] != want {
			t.Fatalf("position %d = %q, want %q (from the snapshot)", i, got[i], want)
		}
	}
	for i := 0; i < 5; i++ {
		if want := fmt.Sprintf("post%d", i); got[15+i] != want {
			t.Fatalf("position %d = %q, want %q (from the log after the boundary)", 15+i, got[15+i], want)
		}
	}
	c.assertNoDivergence()
}

func TestSnapshotBoundaryStillAnswersConsistencyCheck(t *testing.T) {
	// A leader probing at exactly the compaction boundary must get a correct
	// answer. Without the virtual entry standing in for the compacted prefix, that
	// probe would look like a reference to a nonexistent index and force an
	// unnecessary snapshot transfer.
	n := newTestNode(t, 2, NewConfig(1, 2, 3))
	n.log.append(ents(1, 1, 2, 2, 3)...)
	n.log.commitTo(5)
	n.log.appliedTo(5)
	if err := n.log.compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	n.becomeFollower(3, 1)

	if err := n.Step(Message{
		Type: MsgAppReq, From: 1, To: 2, Term: 3,
		PrevLogIndex: 3, PrevLogTerm: 2, // the boundary itself
		Entries: []Entry{{Term: 3, Index: 4}, {Term: 3, Index: 5}},
		Commit:  5,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, n, MsgAppResp)
	if resp.Reject {
		t.Error("a probe at the compaction boundary was rejected")
	}
	if resp.MatchIndex != 5 {
		t.Errorf("MatchIndex = %d, want 5", resp.MatchIndex)
	}
}
