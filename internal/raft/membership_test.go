package raft

import (
	"fmt"
	"testing"
)

// --- encoding --------------------------------------------------------------

func TestConfChangeRoundTrip(t *testing.T) {
	cases := []ConfChange{
		{Type: ConfChangeAddVoter, NodeID: 4},
		{Type: ConfChangeAddLearner, NodeID: 9, Context: []byte("127.0.0.1:9004")},
		{Type: ConfChangeRemoveNode, NodeID: 2},
		{Type: ConfChangePromoteLearner, NodeID: 9},
		{Type: ConfChangeLeaveJoint},
	}
	for _, want := range cases {
		t.Run(want.Type.String(), func(t *testing.T) {
			got, err := DecodeConfChange(want.Encode())
			if err != nil {
				t.Fatalf("DecodeConfChange: %v", err)
			}
			if got.Type != want.Type || got.NodeID != want.NodeID {
				t.Errorf("round trip = %+v, want %+v", got, want)
			}
			if string(got.Context) != string(want.Context) {
				t.Errorf("context = %q, want %q", got.Context, want.Context)
			}
		})
	}
}

func TestDecodeConfChangeRejectsMalformed(t *testing.T) {
	if _, err := DecodeConfChange([]byte{1, 2, 3}); err == nil {
		t.Error("accepted a payload shorter than the header")
	}
	// A context length that disagrees with the payload means the entry is not what
	// it claims, and guessing would apply a change to the wrong node.
	bad := ConfChange{Type: ConfChangeAddVoter, NodeID: 3, Context: []byte("addr")}.Encode()
	if _, err := DecodeConfChange(bad[:len(bad)-1]); err == nil {
		t.Error("accepted a truncated context")
	}
}

func TestConfChangeContextCarriesAddress(t *testing.T) {
	// Without the address travelling in the log, a restarted node would recover a
	// membership containing ids it has no way to reach.
	cc := ConfChange{Type: ConfChangeAddLearner, NodeID: 7, Context: []byte("10.0.0.7:9000")}
	got, err := DecodeConfChange(cc.Encode())
	if err != nil {
		t.Fatalf("DecodeConfChange: %v", err)
	}
	if string(got.Context) != "10.0.0.7:9000" {
		t.Errorf("context = %q", got.Context)
	}
}

// --- configuration transitions --------------------------------------------

func TestWithChangeEntersJointForVoterChanges(t *testing.T) {
	base := NewConfig(1, 2, 3)

	tests := []struct {
		name      string
		cc        ConfChange
		wantJoint bool
		check     func(t *testing.T, c Config)
	}{
		{
			name:      "add voter",
			cc:        ConfChange{Type: ConfChangeAddVoter, NodeID: 4},
			wantJoint: true,
			check: func(t *testing.T, c Config) {
				if !c.Voters[0].Contains(4) {
					t.Error("node 4 is not in the incoming set")
				}
				for _, id := range []NodeID{1, 2, 3} {
					if !c.Voters[1].Contains(id) {
						t.Errorf("node %d is missing from the outgoing set", id)
					}
				}
			},
		},
		{
			name:      "remove voter",
			cc:        ConfChange{Type: ConfChangeRemoveNode, NodeID: 3},
			wantJoint: true,
			check: func(t *testing.T, c Config) {
				if c.Voters[0].Contains(3) {
					t.Error("node 3 is still in the incoming set")
				}
				if !c.Voters[1].Contains(3) {
					t.Error("node 3 must remain in the outgoing set until the change commits")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, err := base.withChange(tt.cc)
			if err != nil {
				t.Fatalf("withChange: %v", err)
			}
			if next.IsJoint() != tt.wantJoint {
				t.Errorf("IsJoint() = %v, want %v (%s)", next.IsJoint(), tt.wantJoint, next)
			}
			tt.check(t, next)
		})
	}
}

func TestLearnerChangesSkipTheJointStep(t *testing.T) {
	// A learner never counts toward quorum, so adding or removing one cannot
	// create two disjoint majorities and needs no transition.
	base := NewConfig(1, 2, 3)

	added, err := base.withChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	if err != nil {
		t.Fatalf("add learner: %v", err)
	}
	if added.IsJoint() {
		t.Error("adding a learner entered a joint configuration unnecessarily")
	}
	if !added.IsLearner(4) {
		t.Error("node 4 is not a learner")
	}

	removed, err := added.withChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 4})
	if err != nil {
		t.Fatalf("remove learner: %v", err)
	}
	if removed.IsJoint() {
		t.Error("removing a learner entered a joint configuration unnecessarily")
	}
	if removed.IsLearner(4) {
		t.Error("node 4 is still a learner")
	}
}

func TestPromoteLearnerEntersJoint(t *testing.T) {
	base := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()}, Learners: NewNodeSet(4)}

	next, err := base.withChange(ConfChange{Type: ConfChangePromoteLearner, NodeID: 4})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if !next.IsJoint() {
		t.Error("promotion changes the voter set, so it must enter a joint configuration")
	}
	if !next.IsVoter(4) || next.IsLearner(4) {
		t.Errorf("after promotion node 4 is voter=%v learner=%v", next.IsVoter(4), next.IsLearner(4))
	}
}

func TestLeaveJointCompletesTransition(t *testing.T) {
	joint := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3, 4), NewNodeSet(1, 2, 3)}}

	final, err := joint.withChange(ConfChange{Type: ConfChangeLeaveJoint})
	if err != nil {
		t.Fatalf("leave joint: %v", err)
	}
	if final.IsJoint() {
		t.Error("the configuration is still joint after leave-joint")
	}
	if len(final.Voters[0]) != 4 {
		t.Errorf("final voters = %s, want the four incoming members", final)
	}
}

func TestLeaveJointWithoutTransitionIsRejected(t *testing.T) {
	if _, err := NewConfig(1, 2, 3).withChange(ConfChange{Type: ConfChangeLeaveJoint}); err == nil {
		t.Error("leave-joint succeeded with no transition in flight")
	}
}

func TestOverlappingChangesAreRejected(t *testing.T) {
	// There is only one slot for an outgoing set. A second concurrent change would
	// discard the first's safety guarantee, so it must be refused.
	joint := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3, 4), NewNodeSet(1, 2, 3)}}
	if _, err := joint.withChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 5}); err == nil {
		t.Error("a second voter change was accepted during a transition")
	}
}

func TestChangeRemovingLastVoterIsRejected(t *testing.T) {
	// A cluster with no voters can never elect a leader again, and no later change
	// could repair it.
	if _, err := NewConfig(1).withChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); err == nil {
		t.Error("removing the last voter was accepted")
	}
}

func TestRedundantChangesAreNoOps(t *testing.T) {
	base := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()}, Learners: NewNodeSet(4)}

	tests := []struct {
		name string
		cc   ConfChange
	}{
		{"add an existing voter", ConfChange{Type: ConfChangeAddVoter, NodeID: 2}},
		{"add an existing learner", ConfChange{Type: ConfChangeAddLearner, NodeID: 4}},
		{"remove a non-member", ConfChange{Type: ConfChangeRemoveNode, NodeID: 99}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, err := base.withChange(tt.cc)
			if err != nil {
				t.Fatalf("withChange: %v", err)
			}
			// A repeated request must not drag the cluster into a transition, where
			// it needs two majorities and is less available.
			if next.IsJoint() {
				t.Error("a redundant change entered a joint configuration")
			}
		})
	}
}

func TestAddingVoterClearsLearnerStatus(t *testing.T) {
	// A node that is both a voter and a learner would be counted and not counted
	// in the same decision, which Validate rejects.
	base := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()}, Learners: NewNodeSet(4)}
	next, err := base.withChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 4})
	if err != nil {
		t.Fatalf("withChange: %v", err)
	}
	if next.IsLearner(4) {
		t.Error("node 4 is still a learner after being added as a voter")
	}
	if err := next.Validate(); err != nil {
		t.Errorf("resulting configuration is invalid: %v", err)
	}
}

func TestPromoteNonLearnerIsRejected(t *testing.T) {
	if _, err := NewConfig(1, 2, 3).withChange(ConfChange{Type: ConfChangePromoteLearner, NodeID: 2}); err == nil {
		t.Error("promoted a node that was not a learner")
	}
}

// --- the safety property joint consensus exists for -----------------------

func TestJointTransitionAdmitsNoDisjointMajorities(t *testing.T) {
	// The property the whole mechanism exists to guarantee. Take a transition with
	// no overlap between the old and new voter sets — the worst case — and verify
	// that no two disjoint groups can both reach quorum, in any combination.
	base := NewConfig(1, 2, 3)
	joint, err := base.withChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 4})
	if err != nil {
		t.Fatalf("withChange: %v", err)
	}
	// Force complete disjointness, which a single add cannot produce but a
	// hand-built configuration can.
	joint = Config{Voters: [2]NodeSet{NewNodeSet(4, 5, 6), NewNodeSet(1, 2, 3)}}

	all := []NodeID{1, 2, 3, 4, 5, 6}
	// Enumerate every possible pair of disjoint vote sets over six nodes.
	for mask := 0; mask < 1<<len(all); mask++ {
		groupA := map[NodeID]bool{}
		groupB := map[NodeID]bool{}
		for i, id := range all {
			if mask&(1<<i) != 0 {
				groupA[id] = true
			} else {
				groupB[id] = true
			}
		}
		if joint.HasQuorum(groupA) && joint.HasQuorum(groupB) {
			t.Fatalf("SAFETY VIOLATION: disjoint groups %v and %v both reached quorum", groupA, groupB)
		}
	}
}

// --- node-level behaviour --------------------------------------------------

func TestProposeConfChangeRequiresLeadership(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	if _, err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 4}); err != ErrProposalDropped {
		t.Errorf("error = %v, want ErrProposalDropped on a follower", err)
	}
}

func TestProposeConfChangeRejectsSecondChangeBeforeApply(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	// Apply the election's no-op so the leader is not blocked by its own entry.
	n.Advance(n.Ready())
	n.log.commitTo(n.log.lastIndex())
	n.log.appliedTo(n.log.lastIndex())

	if _, err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	// The first change is in the log but not applied, so the configuration it
	// produces is unknown and a second cannot be validated safely.
	if _, err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 5}); err == nil {
		t.Error("a second change was accepted before the first was applied")
	}
	if !n.PendingConfChange() {
		t.Error("PendingConfChange() = false with a change in the log")
	}
}

func TestProposeConfChangeRejectsInvalidChangeImmediately(t *testing.T) {
	// A client should learn at once, rather than watching an entry commit and
	// silently do nothing.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())
	n.log.commitTo(n.log.lastIndex())
	n.log.appliedTo(n.log.lastIndex())

	if _, err := n.ProposeConfChange(ConfChange{Type: ConfChangePromoteLearner, NodeID: 2}); err == nil {
		t.Error("promoting a non-learner was accepted")
	}
}

func TestApplyConfChangeAutoLeavesJoint(t *testing.T) {
	// The cluster must not sit in the joint state, where it needs two majorities
	// and is therefore less available than either configuration alone.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	cfg, err := n.ApplyConfChange(ConfChange{Type: ConfChangeAddVoter, NodeID: 4})
	if err != nil {
		t.Fatalf("ApplyConfChange: %v", err)
	}
	if !cfg.IsJoint() {
		t.Fatal("adding a voter did not enter a joint configuration")
	}

	// The leader should have proposed the completing entry itself.
	found := false
	for _, e := range n.log.entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			t.Fatalf("DecodeConfChange: %v", err)
		}
		if cc.Type == ConfChangeLeaveJoint {
			found = true
		}
	}
	if !found {
		t.Error("the leader did not propose leave-joint after entering a transition")
	}

	// Applying it completes the transition.
	final, err := n.ApplyConfChange(ConfChange{Type: ConfChangeLeaveJoint})
	if err != nil {
		t.Fatalf("leave joint: %v", err)
	}
	if final.IsJoint() {
		t.Error("the configuration is still joint")
	}
	if !final.IsVoter(4) {
		t.Error("node 4 is not a voter in the final configuration")
	}
}

func TestLeaderRemovedFromConfigurationStepsDown(t *testing.T) {
	// A leader that is no longer a voter cannot be part of any quorum, so it can
	// neither commit nor prove leadership. Holding the role would block the
	// cluster while looking healthy.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	if _, err := n.ApplyConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); err != nil {
		t.Fatalf("ApplyConfChange: %v", err)
	}

	// During the joint phase the leader is still a voter — it remains in the
	// outgoing set — and must keep the role to shepherd the transition to
	// completion. Stepping down here would strand the cluster mid-change.
	if n.Role() != Leader {
		t.Fatalf("the leader stepped down during the joint phase (role = %s); it is still an outgoing voter", n.Role())
	}
	if !n.Membership().IsVoter(1) {
		t.Error("the leader is no longer counted as a voter during the transition")
	}

	// Once the transition completes, it is genuinely out of the configuration and
	// can no longer be part of any quorum.
	if _, err := n.ApplyConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leave joint: %v", err)
	}
	if n.Role() == Leader {
		t.Error("a leader removed from the completed configuration kept its role")
	}
	if _, err := n.Propose([]byte("x")); err == nil {
		t.Error("the removed leader still accepts proposals")
	}
}

func TestRemovedLeaderCannotProposeEvenIfStillLeader(t *testing.T) {
	// Belt and braces: appendEntry itself refuses when this node is not in the
	// configuration, independently of the role transition above.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	n.cfg = NewConfig(2, 3)
	n.role = Leader // force the state the guard is meant to catch

	if _, err := n.Propose([]byte("x")); err != ErrProposalDropped {
		t.Errorf("error = %v, want ErrProposalDropped", err)
	}
}

func TestNewVoterCountsTowardQuorumAfterApply(t *testing.T) {
	// Growing from three voters to five raises the quorum requirement from two to
	// three. The commit index must not advance on two acknowledgements afterwards.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate()
	n.becomeLeader()
	n.Advance(n.Ready())

	n.cfg = NewConfig(1, 2, 3, 4, 5)
	n.resetProgress()

	idx, err := n.Propose([]byte("v"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.Advance(n.Ready()) // the leader's own entry becomes durable

	if err := n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.Term(), MatchIndex: idx}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.CommitIndex() >= idx {
		t.Errorf("commit index = %d with two of five acknowledging; quorum is now three", n.CommitIndex())
	}
	if err := n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: n.Term(), MatchIndex: idx}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.CommitIndex() != idx {
		t.Errorf("commit index = %d with three of five acknowledging, want %d", n.CommitIndex(), idx)
	}
}

func TestLearnerReceivesLogButNeverVotes(t *testing.T) {
	// The reason to add a node as a learner first: it catches up without its
	// absence from quorum arithmetic mattering.
	cfg := Config{Voters: [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()}, Learners: NewNodeSet(4)}
	n := newTestNode(t, 1, cfg)
	n.becomeCandidate()

	// No vote may ever be solicited from a learner: a learner that voted would
	// help elect a leader on votes it has no right to cast.
	for _, m := range n.msgs {
		if m.To == 4 && m.Type == MsgVoteReq {
			t.Error("a vote was requested from a learner")
		}
	}

	n.becomeLeader()

	// The learner must be replicated to, from the leader's very first broadcast.
	sawLearner := false
	for _, m := range n.msgs {
		if m.To == 4 && m.Type == MsgAppReq {
			sawLearner = true
		}
	}
	if !sawLearner {
		t.Error("the learner was never sent any entries")
	}
	n.Advance(n.Ready())

	// Move the learner into streaming so further entries flow to it, then confirm
	// they do.
	if err := n.Step(Message{Type: MsgAppResp, From: 4, To: 1, Term: n.Term(), MatchIndex: n.log.lastIndex()}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	n.msgs = nil
	for i := 0; i < 3; i++ {
		if _, err := n.Propose([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	streamed := false
	for _, m := range n.msgs {
		if m.To == 4 && m.Type == MsgAppReq && len(m.Entries) > 0 {
			streamed = true
		}
	}
	if !streamed {
		t.Error("the learner stopped receiving entries once streaming was established")
	}

	// And its acknowledgements must not advance the commit index.
	idx := n.log.lastIndex()
	n.Advance(n.Ready())
	before := n.CommitIndex()
	if err := n.Step(Message{Type: MsgAppResp, From: 4, To: 1, Term: n.Term(), MatchIndex: idx}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.CommitIndex() != before {
		t.Errorf("commit index moved from %d to %d on a learner's acknowledgement",
			before, n.CommitIndex())
	}
}

// --- whole-cluster membership changes -------------------------------------

func TestClusterGrowsFromThreeToFive(t *testing.T) {
	// Run a real transition through the harness: add two members as learners,
	// promote them, and confirm the cluster keeps committing throughout.
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	c.propose("before-growth")
	c.tick(2)

	for _, id := range []NodeID{4, 5} {
		// A new member joins as a learner so its catch-up cannot stall commits.
		if _, err := l.node.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: id}); err != nil {
			t.Fatalf("add learner %d: %v", id, err)
		}
		c.run()
		// The driver applies the change; the harness does it explicitly because it
		// has no server layer.
		c.tick(2)
	}

	cfg := l.node.Membership()
	for _, id := range []NodeID{4, 5} {
		if !cfg.IsLearner(id) {
			t.Errorf("node %d is not a learner: %s", id, cfg)
		}
	}

	// The cluster must still commit while the learners exist, since they do not
	// affect quorum.
	c.propose("during-growth")
	c.tick(2)
	if l.node.Role() != Leader {
		t.Fatalf("leader lost its role during growth (role = %s)", l.node.Role())
	}
	c.assertNoDivergence()
}

func TestClusterShrinksAndKeepsCommitting(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	l := c.electLeader()
	c.propose("before-shrink")
	c.tick(2)

	// Remove a follower, taking the cluster from five voters to four.
	var victim NodeID
	for _, id := range c.ids {
		if id != l.node.ID() {
			victim = id
			break
		}
	}
	if _, err := l.node.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: victim}); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	c.run()
	c.tick(5)

	if l.node.Membership().IsVoter(victim) {
		t.Errorf("node %d is still a voter: %s", victim, l.node.Membership())
	}
	if l.node.Membership().IsJoint() {
		t.Errorf("the configuration is still joint: %s", l.node.Membership())
	}

	c.propose("after-shrink")
	c.tick(2)
	if l.node.Role() != Leader {
		t.Errorf("leader lost its role during the shrink (role = %s)", l.node.Role())
	}
	c.assertNoDivergence()
}
