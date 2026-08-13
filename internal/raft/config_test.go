package raft

import "testing"

func votes(granted ...NodeID) map[NodeID]bool {
	m := make(map[NodeID]bool, len(granted))
	for _, id := range granted {
		m[id] = true
	}
	return m
}

func TestConfigQuorumSimple(t *testing.T) {
	three := NewConfig(1, 2, 3)
	five := NewConfig(1, 2, 3, 4, 5)

	tests := []struct {
		name string
		cfg  Config
		v    map[NodeID]bool
		want bool
	}{
		{"3-node: one vote", three, votes(1), false},
		{"3-node: two votes", three, votes(1, 2), true},
		{"3-node: all three", three, votes(1, 2, 3), true},
		{"3-node: no votes", three, votes(), false},
		{"5-node: two votes", five, votes(1, 2), false},
		{"5-node: three votes", five, votes(1, 4, 5), true},
		// Votes from non-members must not be counted toward quorum.
		{"3-node: outsider votes ignored", three, votes(1, 99, 98), false},
	}
	for _, tt := range tests {
		if got := tt.cfg.HasQuorum(tt.v); got != tt.want {
			t.Errorf("%s: HasQuorum() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestConfigQuorumEvenSizedCluster(t *testing.T) {
	// A 4-node cluster needs 3, not 2: allowing a tie would let two halves of a
	// partition each believe they hold a majority.
	four := NewConfig(1, 2, 3, 4)
	if four.HasQuorum(votes(1, 2)) {
		t.Error("HasQuorum() accepted a 2-of-4 tie")
	}
	if !four.HasQuorum(votes(1, 2, 3)) {
		t.Error("HasQuorum() rejected 3 of 4")
	}
}

func TestConfigEmptyVotersHasNoQuorum(t *testing.T) {
	// An unconfigured node must not be able to elect itself.
	var empty Config
	if empty.HasQuorum(votes(1, 2, 3)) {
		t.Error("HasQuorum() = true for a configuration with no voters")
	}
}

func TestConfigJointQuorumRequiresBothMajorities(t *testing.T) {
	// Transition from {1,2,3} to {3,4,5}. Node 3 is the only overlap, so the
	// two sets can otherwise be satisfied by disjoint groups.
	joint := Config{Voters: [2]NodeSet{NewNodeSet(3, 4, 5), NewNodeSet(1, 2, 3)}}
	if !joint.IsJoint() {
		t.Fatal("IsJoint() = false during a transition")
	}

	tests := []struct {
		name string
		v    map[NodeID]bool
		want bool
	}{
		// A majority of the union {1..5} that is a majority of neither set.
		{"union majority only", votes(1, 2, 4), false},
		{"incoming majority only", votes(4, 5), false},
		{"outgoing majority only", votes(1, 2), false},
		{"majority in both", votes(1, 2, 3), false}, // 3,4,5: only node 3
		{"majority in both via overlap", votes(1, 3, 4), true},
		{"everyone", votes(1, 2, 3, 4, 5), true},
	}
	for _, tt := range tests {
		if got := joint.HasQuorum(tt.v); got != tt.want {
			t.Errorf("%s: HasQuorum(%v) = %v, want %v", tt.name, tt.v, got, tt.want)
		}
	}
}

func TestConfigJointQuorumBlocksDisjointLeaders(t *testing.T) {
	// The property joint consensus exists to guarantee: no two disjoint groups
	// can each reach quorum, so a transition can never yield two leaders.
	joint := Config{Voters: [2]NodeSet{NewNodeSet(4, 5, 6), NewNodeSet(1, 2, 3)}}
	oldGroup := votes(1, 2, 3)
	newGroup := votes(4, 5, 6)
	if joint.HasQuorum(oldGroup) && joint.HasQuorum(newGroup) {
		t.Error("both the old and new configurations reached quorum independently")
	}
	if joint.HasQuorum(oldGroup) || joint.HasQuorum(newGroup) {
		t.Error("a single-side majority reached quorum during a joint transition")
	}
}

func TestConfigCommittedIndex(t *testing.T) {
	three := NewConfig(1, 2, 3)

	tests := []struct {
		name  string
		cfg   Config
		match map[NodeID]Index
		want  Index
	}{
		// Sorted descending: 10, 8, 5 -> median 8 is on a majority (nodes 1,2).
		{"3-node median", three, map[NodeID]Index{1: 10, 2: 8, 3: 5}, 8},
		{"3-node all equal", three, map[NodeID]Index{1: 7, 2: 7, 3: 7}, 7},
		// One node far ahead cannot commit alone.
		{"3-node one ahead", three, map[NodeID]Index{1: 99, 2: 0, 3: 0}, 0},
		{"5-node", NewConfig(1, 2, 3, 4, 5), map[NodeID]Index{1: 9, 2: 9, 3: 7, 4: 2, 5: 0}, 7},
		// Missing entries read as index 0: a node that has acknowledged nothing.
		{"3-node missing match", three, map[NodeID]Index{1: 10, 2: 10}, 10},
		{"no voters", Config{}, map[NodeID]Index{1: 10}, 0},
	}
	for _, tt := range tests {
		if got := tt.cfg.CommittedIndex(tt.match); got != tt.want {
			t.Errorf("%s: CommittedIndex() = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestConfigCommittedIndexJointTakesLowerMajority(t *testing.T) {
	// Incoming {3,4,5} has replicated far; outgoing {1,2,3} lags. An entry is
	// only committed once *both* configurations have it on a majority.
	joint := Config{Voters: [2]NodeSet{NewNodeSet(3, 4, 5), NewNodeSet(1, 2, 3)}}
	match := map[NodeID]Index{1: 4, 2: 4, 3: 20, 4: 20, 5: 20}
	if got := joint.CommittedIndex(match); got != 4 {
		t.Errorf("CommittedIndex() = %d, want 4 (limited by the outgoing majority)", got)
	}
}

func TestConfigLearnersDoNotCountTowardQuorum(t *testing.T) {
	cfg := Config{
		Voters:   [2]NodeSet{NewNodeSet(1, 2, 3), NewNodeSet()},
		Learners: NewNodeSet(4, 5),
	}
	if cfg.HasQuorum(votes(1, 4, 5)) {
		t.Error("HasQuorum() counted learner votes")
	}
	if got := cfg.CommittedIndex(map[NodeID]Index{1: 9, 2: 1, 3: 1, 4: 9, 5: 9}); got != 1 {
		t.Errorf("CommittedIndex() = %d, want 1: learners must not advance the commit index", got)
	}
	if !cfg.IsLearner(4) || cfg.IsVoter(4) {
		t.Error("node 4 should be a learner and not a voter")
	}
	// Learners still receive the log, so they must appear in Members.
	if got := len(cfg.Members()); got != 5 {
		t.Errorf("len(Members()) = %d, want 5", got)
	}
	if got := len(cfg.VoterIDs()); got != 3 {
		t.Errorf("len(VoterIDs()) = %d, want 3", got)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"simple", NewConfig(1, 2, 3), false},
		{"with learners", Config{Voters: [2]NodeSet{NewNodeSet(1), NewNodeSet()}, Learners: NewNodeSet(2)}, false},
		{"no voters", Config{}, true},
		{"voter is also a learner", Config{Voters: [2]NodeSet{NewNodeSet(1, 2), NewNodeSet()}, Learners: NewNodeSet(2)}, true},
		{"reserved id as voter", NewConfig(0, 1), true},
		{"reserved id as learner", Config{Voters: [2]NodeSet{NewNodeSet(1), NewNodeSet()}, Learners: NewNodeSet(0)}, true},
	}
	for _, tt := range tests {
		err := tt.cfg.Validate()
		if (err != nil) != tt.wantErr {
			t.Errorf("%s: Validate() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestConfigCloneIsDeep(t *testing.T) {
	orig := Config{Voters: [2]NodeSet{NewNodeSet(1, 2), NewNodeSet(3)}, Learners: NewNodeSet(4)}
	clone := orig.Clone()
	delete(clone.Voters[0], 1)
	delete(clone.Voters[1], 3)
	delete(clone.Learners, 4)

	if !orig.Voters[0].Contains(1) || !orig.Voters[1].Contains(3) || !orig.Learners.Contains(4) {
		t.Error("mutating the clone changed the original configuration")
	}
}

func TestNodeSetSortedIsDeterministic(t *testing.T) {
	s := NewNodeSet(9, 3, 7, 1)
	want := []NodeID{1, 3, 7, 9}
	for i := 0; i < 20; i++ {
		got := s.Sorted()
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("Sorted() = %v, want %v", got, want)
			}
		}
	}
}
