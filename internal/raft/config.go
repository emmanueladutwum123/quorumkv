package raft

import (
	"fmt"
	"sort"
	"strings"
)

// NodeSet is an unordered set of cluster members.
type NodeSet map[NodeID]struct{}

// NewNodeSet builds a set from the given ids.
func NewNodeSet(ids ...NodeID) NodeSet {
	s := make(NodeSet, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

// Contains reports set membership.
func (s NodeSet) Contains(id NodeID) bool {
	_, ok := s[id]
	return ok
}

// Sorted returns the members in ascending order, so that anything derived from
// a set — log output, config-change payloads, test assertions — is stable
// rather than dependent on Go's randomised map iteration.
func (s NodeSet) Sorted() []NodeID {
	out := make([]NodeID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Clone returns an independent copy.
func (s NodeSet) Clone() NodeSet {
	out := make(NodeSet, len(s))
	for id := range s {
		out[id] = struct{}{}
	}
	return out
}

func (s NodeSet) String() string {
	ids := s.Sorted()
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(uint64(id))
	}
	return "(" + strings.Join(parts, " ") + ")"
}

// Config is the cluster membership.
//
// Voters holds up to two voter sets to support joint consensus (§6). Voters[0]
// is the incoming configuration, always in force. Voters[1] is the outgoing
// configuration and is non-empty only while a membership change is in flight;
// during that window every quorum decision must be satisfied in *both* sets.
// That double requirement is precisely what prevents the old and new majorities
// from independently electing two leaders during a transition.
//
// Learners receive the log but neither vote nor count toward quorum. A node
// joins as a learner so that its initial catch-up — which can take as long as
// transferring a full snapshot — cannot stall the cluster's ability to commit.
type Config struct {
	Voters   [2]NodeSet
	Learners NodeSet
}

// NewConfig builds a simple (non-joint) configuration from a voter list.
func NewConfig(voters ...NodeID) Config {
	return Config{Voters: [2]NodeSet{NewNodeSet(voters...), NewNodeSet()}}
}

// IsJoint reports whether a membership change is currently in flight.
func (c Config) IsJoint() bool {
	return len(c.Voters[1]) > 0
}

// IsVoter reports whether id may vote and counts toward quorum. During a joint
// transition, membership in either set confers voting rights: the outgoing
// members have not yet been retired.
func (c Config) IsVoter(id NodeID) bool {
	return c.Voters[0].Contains(id) || c.Voters[1].Contains(id)
}

// IsLearner reports whether id replicates the log without voting.
func (c Config) IsLearner(id NodeID) bool {
	return c.Learners.Contains(id)
}

// Contains reports whether id participates in the cluster at all.
func (c Config) Contains(id NodeID) bool {
	return c.IsVoter(id) || c.IsLearner(id)
}

// Voters returns every voting member across both sets, sorted.
func (c Config) VoterIDs() []NodeID {
	union := c.Voters[0].Clone()
	for id := range c.Voters[1] {
		union[id] = struct{}{}
	}
	return union.Sorted()
}

// Members returns every voter and learner, sorted. This is the set the leader
// replicates to.
func (c Config) Members() []NodeID {
	union := c.Voters[0].Clone()
	for id := range c.Voters[1] {
		union[id] = struct{}{}
	}
	for id := range c.Learners {
		union[id] = struct{}{}
	}
	return union.Sorted()
}

// HasQuorum reports whether the nodes marked true in votes constitute a
// majority. In a joint configuration a majority is required in both voter sets
// independently; a majority of the union is not sufficient and accepting it
// would reintroduce the split-brain the joint step exists to prevent.
func (c Config) HasQuorum(votes map[NodeID]bool) bool {
	if !hasMajority(c.Voters[0], votes) {
		return false
	}
	if c.IsJoint() && !hasMajority(c.Voters[1], votes) {
		return false
	}
	return true
}

func hasMajority(voters NodeSet, votes map[NodeID]bool) bool {
	if len(voters) == 0 {
		// An empty voter set cannot produce a majority. Treating it as
		// satisfied would let a node with no configuration elect itself.
		return false
	}
	granted := 0
	for id := range voters {
		if votes[id] {
			granted++
		}
	}
	return granted*2 > len(voters)
}

// CommittedIndex returns the highest index replicated to a majority, given each
// voter's match index. Entries at or below this index are safe to commit.
//
// The value is the median match index of each voter set (taking the lower of
// the two while joint), because sorting the match indexes descending and
// stepping to position len/2 finds the highest index that a majority has
// reached or exceeded.
func (c Config) CommittedIndex(match map[NodeID]Index) Index {
	committed := majorityIndex(c.Voters[0], match)
	if c.IsJoint() {
		if outgoing := majorityIndex(c.Voters[1], match); outgoing < committed {
			committed = outgoing
		}
	}
	return committed
}

func majorityIndex(voters NodeSet, match map[NodeID]Index) Index {
	if len(voters) == 0 {
		return 0
	}
	idx := make([]Index, 0, len(voters))
	for id := range voters {
		idx = append(idx, match[id])
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] > idx[j] })
	return idx[len(idx)/2]
}

// Clone returns a deep copy, so that a configuration captured in a snapshot or
// a pending change cannot be mutated through the original.
func (c Config) Clone() Config {
	out := Config{Learners: c.Learners.Clone()}
	out.Voters[0] = c.Voters[0].Clone()
	out.Voters[1] = c.Voters[1].Clone()
	return out
}

func (c Config) String() string {
	var b strings.Builder
	b.WriteString("voters=")
	b.WriteString(c.Voters[0].String())
	if c.IsJoint() {
		b.WriteString("&&")
		b.WriteString(c.Voters[1].String())
	}
	if len(c.Learners) > 0 {
		b.WriteString(" learners=")
		b.WriteString(c.Learners.String())
	}
	return b.String()
}

// Validate reports whether the configuration is structurally sound. It catches
// the two states that would make quorum arithmetic meaningless: no voters at
// all, and a node that is simultaneously a voter and a learner (which would let
// it be counted and not counted in the same decision).
func (c Config) Validate() error {
	if len(c.Voters[0]) == 0 {
		return fmt.Errorf("raft: configuration has no voters")
	}
	for id := range c.Learners {
		if c.Voters[0].Contains(id) || c.Voters[1].Contains(id) {
			return fmt.Errorf("raft: node %d is both a voter and a learner", id)
		}
	}
	if _, ok := c.Learners[None]; ok {
		return fmt.Errorf("raft: node id 0 is reserved")
	}
	for _, id := range c.VoterIDs() {
		if id == None {
			return fmt.Errorf("raft: node id 0 is reserved")
		}
	}
	return nil
}
