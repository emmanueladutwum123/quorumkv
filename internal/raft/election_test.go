package raft

import (
	"math/rand/v2"
	"testing"
)

// --- basic elections -------------------------------------------------------

func TestSingleNodeElectsItself(t *testing.T) {
	c := newCluster(t, []NodeID{1})
	l := c.electLeader()

	if l.node.ID() != 1 {
		t.Fatalf("leader = %d, want 1", l.node.ID())
	}
	// A single voter is its own quorum, so the no-op committed on election is
	// enough to establish the commit index with no messages exchanged at all.
	if l.node.CommitIndex() != 1 {
		t.Errorf("commit index = %d, want 1 (the leader's no-op)", l.node.CommitIndex())
	}
	if len(c.queue) != 0 {
		t.Errorf("a single-node cluster sent %d messages, want 0", len(c.queue))
	}
}

func TestThreeNodesElectExactlyOneLeader(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("%d nodes claim leadership, want 1", got)
	}
	term := l.node.Term()
	for _, id := range c.ids {
		p := c.peers[id]
		if p.node.Term() != term {
			t.Errorf("node %d term = %d, want %d", id, p.node.Term(), term)
		}
		if p.node.Leader() != l.node.ID() {
			t.Errorf("node %d thinks the leader is %d, want %d", id, p.node.Leader(), l.node.ID())
		}
		if id != l.node.ID() && p.node.Role() != Follower {
			t.Errorf("node %d role = %s, want follower", id, p.node.Role())
		}
	}
}

func TestLeaderCommitsNoOpOnElection(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	// §5.4.2: the new leader must commit an entry of its own term before it can
	// trust its commit index. It does so with a blank entry rather than waiting
	// for a client write.
	if l.node.CommitIndex() == 0 {
		t.Fatal("leader did not commit its no-op entry")
	}
	if !l.node.committedInCurrentTerm() {
		t.Error("committedInCurrentTerm() = false after election")
	}
	found := false
	for _, e := range l.applied {
		if e.Type == EntryNoOp && e.Term == l.node.Term() {
			found = true
		}
	}
	if !found {
		t.Error("no no-op entry of the current term was applied")
	}
}

func TestFiveNodesTolerateTwoFailures(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	l := c.electLeader()

	// Kill a minority: three of five remain, which is still a majority.
	victims := []NodeID{}
	for _, id := range c.ids {
		if id != l.node.ID() && len(victims) < 2 {
			victims = append(victims, id)
		}
	}
	for _, id := range victims {
		c.crash(id)
	}

	c.propose("survives")
	c.tick(2)

	if l.node.Role() != Leader {
		t.Fatalf("leader stepped down with a majority still reachable (role = %s)", l.node.Role())
	}
	if got := l.appliedData(); len(got) != 1 || got[0] != "survives" {
		t.Errorf("leader applied %v, want [survives]", got)
	}
	c.assertNoDivergence()
}

func TestMinorityCannotElectLeader(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3, 4, 5})
	c.electLeader()

	// Isolate two nodes: a group of two can never reach a majority of five, no
	// matter how long it campaigns.
	c.partition([]NodeID{4, 5}, []NodeID{1, 2, 3})
	minorityStart := c.peers[4].node.Term()
	c.tick(100)

	for _, id := range []NodeID{4, 5} {
		if role := c.peers[id].node.Role(); role == Leader {
			t.Errorf("node %d became leader from a two-node minority", id)
		}
	}
	// The minority did keep trying — it reaches the pre-candidate state and
	// stalls there, which is what proves the test exercised a real campaign
	// rather than two idle nodes.
	campaigned := false
	for _, id := range []NodeID{4, 5} {
		if c.peers[id].node.Role() == PreCandidate {
			campaigned = true
		}
	}
	if !campaigned {
		t.Error("neither isolated node campaigned, so the test proved nothing")
	}
	// Their terms must be unchanged: the straw poll never succeeds, so pre-vote
	// keeps them from inflating the term while cut off.
	if got := c.peers[4].node.Term(); got != minorityStart {
		t.Errorf("isolated node term moved from %d to %d; pre-vote should hold it steady",
			minorityStart, got)
	}
	// The majority side must retain a working leader throughout.
	majorityLeaders := 0
	for _, id := range []NodeID{1, 2, 3} {
		if c.peers[id].node.Role() == Leader {
			majorityLeaders++
		}
	}
	if majorityLeaders != 1 {
		t.Errorf("majority side has %d leaders, want 1", majorityLeaders)
	}
}

func TestNewLeaderElectedAfterLeaderCrash(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	old := c.electLeader()
	oldID, oldTerm := old.node.ID(), old.node.Term()

	c.propose("before-crash")
	c.crash(oldID)
	c.tick(60)

	fresh := c.leader()
	if fresh.node.ID() == oldID {
		t.Fatal("the crashed node is still considered leader")
	}
	if fresh.node.Term() <= oldTerm {
		t.Errorf("new leader term = %d, want > %d", fresh.node.Term(), oldTerm)
	}
	// The committed write must survive the failover.
	if got := fresh.appliedData(); len(got) == 0 || got[0] != "before-crash" {
		t.Errorf("new leader applied %v, want it to retain [before-crash]", got)
	}
	c.assertNoDivergence()
}

// --- the election restriction (§5.4.1) -------------------------------------

func TestElectionRestrictionRejectsStaleLog(t *testing.T) {
	// Node 3 misses a write while partitioned, then campaigns. It must lose:
	// any majority it needs includes a node holding the entry it lacks.
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	staleID := NodeID(0)
	for _, id := range c.ids {
		if id != l.node.ID() {
			staleID = id
			break
		}
	}

	c.isolate(staleID)
	c.propose("missed-by-stale-node")
	c.tick(5)

	stale := c.peers[staleID]
	if stale.node.LastIndex() >= l.node.LastIndex() {
		t.Fatalf("isolation failed: stale node is at index %d, leader at %d",
			stale.node.LastIndex(), l.node.LastIndex())
	}

	// Heal, then have the stale node campaign. It has a shorter log, so the
	// other two must refuse.
	c.heal()
	stale.node.Campaign()
	c.run()

	if stale.node.Role() == Leader {
		t.Fatal("SAFETY VIOLATION: a node missing a committed entry won an election")
	}
}

func TestVoteGrantedOnlyToUpToDateCandidate(t *testing.T) {
	// Direct state-machine test: a voter with entries through term 2 must refuse
	// a candidate whose log ends earlier, and accept one at least as current.
	tests := []struct {
		name         string
		lastLogIndex Index
		lastLogTerm  Term
		wantGranted  bool
	}{
		{"candidate behind on term", 3, 1, false},
		{"candidate behind on index", 2, 2, false},
		{"candidate identical", 3, 2, true},
		{"candidate ahead on index", 4, 2, true},
		{"candidate ahead on term", 1, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newTestNode(t, 1, NewConfig(1, 2, 3))
			n.log.append(ents(1, 2, 2)...)

			if err := n.Step(Message{Type: MsgVoteReq, From: 2, To: 1, Term: 5,
				LastLogIndex: tt.lastLogIndex, LastLogTerm: tt.lastLogTerm}); err != nil {
				t.Fatalf("Step: %v", err)
			}
			resp := lastMessage(t, n, MsgVoteResp)
			if resp.Granted != tt.wantGranted {
				t.Errorf("granted = %v, want %v", resp.Granted, tt.wantGranted)
			}
		})
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))

	// Node 2 asks first and, with an equally current log, is granted the vote.
	if err := n.Step(Message{Type: MsgVoteReq, From: 2, To: 1, Term: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp := lastMessage(t, n, MsgVoteResp); !resp.Granted {
		t.Fatal("first vote request was refused")
	}
	if n.vote != 2 {
		t.Fatalf("vote = %d, want 2", n.vote)
	}

	// Node 3 asks in the same term and must be refused: granting both is how two
	// leaders appear in one term.
	if err := n.Step(Message{Type: MsgVoteReq, From: 3, To: 1, Term: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp := lastMessage(t, n, MsgVoteResp); resp.Granted {
		t.Error("SAFETY VIOLATION: a second candidate was granted a vote in the same term")
	}

	// A repeat from the original candidate is granted again, so a lost response
	// does not cost the candidate the election.
	if err := n.Step(Message{Type: MsgVoteReq, From: 2, To: 1, Term: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp := lastMessage(t, n, MsgVoteResp); !resp.Granted {
		t.Error("a retry from the same candidate was refused; vote granting must be idempotent")
	}
}

func TestVoteClearedOnNewTerm(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))

	if err := n.Step(Message{Type: MsgVoteReq, From: 2, To: 1, Term: 2}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.vote != 2 {
		t.Fatalf("vote = %d, want 2", n.vote)
	}

	// A higher term is a fresh election; the old vote must not carry over or
	// this node would be unable to vote in the new term.
	if err := n.Step(Message{Type: MsgVoteReq, From: 3, To: 1, Term: 3}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.vote != 3 {
		t.Errorf("vote = %d, want 3: a vote belongs to the term it was cast in", n.vote)
	}
	if n.term != 3 {
		t.Errorf("term = %d, want 3", n.term)
	}
}

// --- pre-vote (§9.6) -------------------------------------------------------

func TestPreVoteDoesNotDisruptEstablishedLeader(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	leaderID := l.node.ID()

	// Isolate a follower and let its election timer fire many times. With
	// pre-vote, each attempt is a straw poll that changes nothing.
	var victim NodeID
	for _, id := range c.ids {
		if id != leaderID {
			victim = id
			break
		}
	}
	c.isolate(victim)
	termBefore := c.peers[victim].node.Term()
	c.tick(120)

	if got := c.peers[victim].node.Term(); got != termBefore {
		t.Errorf("isolated node term rose from %d to %d; pre-vote must not increment the term",
			termBefore, got)
	}

	// On healing, the returning node must rejoin as a follower without forcing
	// an election.
	leaderTermBefore := l.node.Term()
	c.heal()
	c.tick(20)

	if l.node.Role() != Leader {
		t.Errorf("leader %d was deposed by a returning node (role = %s)", leaderID, l.node.Role())
	}
	if l.node.Term() != leaderTermBefore {
		t.Errorf("leader term rose from %d to %d; the returning node forced a needless election",
			leaderTermBefore, l.node.Term())
	}
	if got := c.peers[victim].node.Role(); got != Follower {
		t.Errorf("returning node role = %s, want follower", got)
	}
}

func TestWithoutPreVoteReturningNodeDisruptsCluster(t *testing.T) {
	// The contrast case: with pre-vote off, an isolated node's repeated
	// campaigns inflate its term, and on healing that term forces the sitting
	// leader to step down even though nothing was wrong. This is the specific
	// harm pre-vote prevents.
	c := newCluster(t, []NodeID{1, 2, 3}, withoutPreVote())
	l := c.electLeader()

	var victim NodeID
	for _, id := range c.ids {
		if id != l.node.ID() {
			victim = id
			break
		}
	}
	c.isolate(victim)
	termBefore := c.peers[victim].node.Term()
	c.tick(120)

	inflated := c.peers[victim].node.Term()
	if inflated <= termBefore {
		t.Fatalf("isolated node term stayed at %d; expected repeated campaigns to raise it", inflated)
	}
	if inflated <= l.node.Term() {
		t.Fatalf("isolated node term %d did not exceed the leader's %d", inflated, l.node.Term())
	}

	c.heal()
	c.tick(20)

	// The returning node's inflated term forces the healthy leader out, costing
	// the cluster an election it had no reason to hold.
	if l.node.Term() < inflated {
		t.Errorf("leader term = %d, expected it to be dragged up to at least %d", l.node.Term(), inflated)
	}
}

func TestPreVoteRequestDoesNotChangeRecipientState(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeFollower(5, 2) // following node 2 in term 5

	err := n.Step(Message{Type: MsgVoteReq, From: 3, To: 1, Term: 9, PreVote: true})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}

	if n.term != 5 {
		t.Errorf("term = %d, want 5: a pre-vote must not advance the recipient's term", n.term)
	}
	if n.lead != 2 {
		t.Errorf("leader = %d, want 2: a pre-vote must not depose the known leader", n.lead)
	}
	if n.vote != None {
		t.Errorf("vote = %d, want None: a pre-vote must not consume the recipient's vote", n.vote)
	}
	resp := lastMessage(t, n, MsgVoteResp)
	if !resp.PreVote {
		t.Error("response did not echo pre_vote; a straw poll must not be mistaken for a real vote")
	}
	if resp.Term != 9 {
		t.Errorf("response term = %d, want 9 (the term asked about)", resp.Term)
	}
}

func TestPreVoteRejectedForStaleCandidateTeachesTerm(t *testing.T) {
	// A node returning from a partition polls with a term below the cluster's.
	// The rejection carries the real term, which is how it catches up without
	// disturbing anyone.
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeFollower(20, 2)

	if err := n.Step(Message{Type: MsgVoteReq, From: 3, To: 1, Term: 4, PreVote: true}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, n, MsgVoteResp)
	if resp.Granted {
		t.Error("granted a pre-vote to a candidate from a stale term")
	}
	if resp.Term != 20 {
		t.Errorf("rejection term = %d, want 20 so the stale node learns the current term", resp.Term)
	}
}

// --- check quorum ----------------------------------------------------------

func TestCheckQuorumDeposesPartitionedLeader(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()
	leaderID := l.node.ID()

	// Cut the leader off. It cannot commit anything, but without a self-check it
	// would keep believing it leads and would answer reads from stale state.
	c.isolate(leaderID)
	c.tick(30)

	if l.node.Role() == Leader {
		t.Error("a leader that cannot reach a majority did not step down")
	}
	// The majority must have elected a replacement.
	others := 0
	for _, id := range c.ids {
		if id != leaderID && c.peers[id].node.Role() == Leader {
			others++
		}
	}
	if others != 1 {
		t.Errorf("majority side elected %d leaders, want 1", others)
	}
}

func TestWithoutCheckQuorumPartitionedLeaderStaysLeader(t *testing.T) {
	// The contrast case: with the self-check disabled, an isolated leader keeps
	// its role indefinitely. It cannot commit, but it still believes it leads —
	// which is exactly how a stale read gets served.
	c := newCluster(t, []NodeID{1, 2, 3}, withoutCheckQuorum())
	l := c.electLeader()

	c.isolate(l.node.ID())
	c.tick(50)

	if l.node.Role() != Leader {
		t.Errorf("role = %s; without CheckQuorum an isolated leader should keep believing it leads",
			l.node.Role())
	}
	// It cannot actually commit anything, which is what makes serving a read
	// from this state a correctness bug rather than merely stale.
	before := l.node.CommitIndex()
	if _, err := l.node.Propose([]byte("orphaned")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.tick(10)
	if l.node.CommitIndex() != before {
		t.Errorf("commit index moved from %d to %d while isolated; a minority must not commit",
			before, l.node.CommitIndex())
	}
}

func TestLeaderRetainsRoleWhileQuorumReachable(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	// One follower dies; two of three nodes remain, which is still a majority,
	// so the self-check must not fire.
	var victim NodeID
	for _, id := range c.ids {
		if id != l.node.ID() {
			victim = id
			break
		}
	}
	c.crash(victim)
	termBefore := l.node.Term()
	c.tick(60)

	if l.node.Role() != Leader {
		t.Errorf("leader stepped down despite a reachable majority (role = %s)", l.node.Role())
	}
	if l.node.Term() != termBefore {
		t.Errorf("term rose from %d to %d without a real failure", termBefore, l.node.Term())
	}
}

// --- learners --------------------------------------------------------------

func TestLearnerNeverCampaigns(t *testing.T) {
	cfg := Config{Voters: [2]NodeSet{NewNodeSet(1), NewNodeSet()}, Learners: NewNodeSet(2)}
	n := newTestNode(t, 2, cfg)

	// Tick far past any election timeout. A learner that stood for election could
	// win with votes it has no right to cast.
	for i := 0; i < 200; i++ {
		n.Tick()
	}

	if n.Role() != Follower {
		t.Errorf("learner role = %s, want follower", n.Role())
	}
	if n.Term() != 0 {
		t.Errorf("learner term = %d, want 0: a learner must never campaign", n.Term())
	}
	if len(n.msgs) != 0 {
		t.Errorf("learner sent %d messages, want 0", len(n.msgs))
	}
}

// --- leadership transfer ---------------------------------------------------

func TestTimeoutNowTriggersImmediateElection(t *testing.T) {
	c := newCluster(t, []NodeID{1, 2, 3})
	l := c.electLeader()

	var target NodeID
	for _, id := range c.ids {
		if id != l.node.ID() {
			target = id
			break
		}
	}
	termBefore := l.node.Term()

	// Graceful transfer: the target campaigns at once rather than waiting out an
	// election timeout, so the handover costs no availability.
	if err := c.peers[target].node.Step(Message{
		Type: MsgTimeoutNow, From: l.node.ID(), To: target, Term: l.node.Term(),
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	c.run()

	if c.peers[target].node.Role() != Leader {
		t.Fatalf("transfer target role = %s, want leader", c.peers[target].node.Role())
	}
	if c.peers[target].node.Term() <= termBefore {
		t.Errorf("new term = %d, want > %d", c.peers[target].node.Term(), termBefore)
	}
	if got := len(c.leaders()); got != 1 {
		t.Errorf("%d leaders after transfer, want 1", got)
	}
}

// --- stale term handling ---------------------------------------------------

func TestStaleLeaderIsToldTheCurrentTerm(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeFollower(10, 2)

	// A deposed leader still sending appends must be answered with the real
	// term. Silence would leave it retrying until its own timeout, failing every
	// client pointed at it in the meantime.
	if err := n.Step(Message{Type: MsgAppReq, From: 3, To: 1, Term: 4}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	resp := lastMessage(t, n, MsgAppResp)
	if !resp.Reject {
		t.Error("a stale append was not rejected")
	}
	if resp.Term != 10 {
		t.Errorf("rejection term = %d, want 10", resp.Term)
	}
}

func TestStaleResponsesAreIgnored(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	n.becomeCandidate() // term 1
	n.becomeCandidate() // term 2
	before := len(n.msgs)

	// A vote response from an old term must not count toward the current
	// election, or a node could be elected on votes cast for a different term.
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Granted: true}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.Role() == Leader {
		t.Error("SAFETY VIOLATION: a stale vote response elected this node")
	}
	if len(n.msgs) != before {
		t.Error("a stale response produced a reply")
	}
}

func TestMessageWithoutTermIsRejected(t *testing.T) {
	n := newTestNode(t, 1, NewConfig(1, 2, 3))
	if err := n.Step(Message{Type: MsgAppReq, From: 2, To: 1, Term: 0}); err == nil {
		t.Error("Step accepted a message with no term")
	}
}

// --- split votes -----------------------------------------------------------

func TestSplitVoteEventuallyResolves(t *testing.T) {
	// Four nodes is the awkward size: a 2-2 split satisfies nobody. Randomised
	// election timeouts are what break the tie, so this asserts that elections
	// terminate rather than repeating forever.
	c := newCluster(t, []NodeID{1, 2, 3, 4})
	l := c.electLeader()

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("%d leaders, want exactly 1", got)
	}
	// Every node must agree, which is what makes the resolution real rather
	// than one node's private belief.
	for _, id := range c.ids {
		if got := c.peers[id].node.Leader(); got != l.node.ID() {
			t.Errorf("node %d thinks the leader is %d, want %d", id, got, l.node.ID())
		}
	}
}

func TestElectionTimeoutIsRandomised(t *testing.T) {
	// Without randomisation, nodes campaign in lockstep and split the vote
	// indefinitely. Verify the drawn timeout varies and stays in [t, 2t).
	seen := make(map[int]bool)
	rng := rand.New(rand.NewPCG(1, 2))
	n, err := NewNode(Options{
		ID: 1, Config: NewConfig(1, 2, 3),
		ElectionTimeoutTicks: 10, HeartbeatTimeoutTicks: 1,
		Rand: rng.IntN,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	for i := 0; i < 200; i++ {
		n.resetRandomizedElectionTimeout()
		got := n.randomizedElectionTimeout
		if got < n.opts.ElectionTimeoutTicks || got >= 2*n.opts.ElectionTimeoutTicks {
			t.Fatalf("timeout %d outside [%d, %d)", got, n.opts.ElectionTimeoutTicks, 2*n.opts.ElectionTimeoutTicks)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Errorf("drew only %d distinct timeouts across 200 attempts; randomisation is what breaks split votes", len(seen))
	}
}

// --- configuration validation ---------------------------------------------

func TestNewNodeRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"zero id", Options{ID: 0, Config: NewConfig(1)}},
		{"no voters", Options{ID: 1}},
		{"heartbeat at election timeout", Options{
			ID: 1, Config: NewConfig(1), ElectionTimeoutTicks: 5, HeartbeatTimeoutTicks: 5,
		}},
		{"heartbeat above election timeout", Options{
			ID: 1, Config: NewConfig(1), ElectionTimeoutTicks: 5, HeartbeatTimeoutTicks: 9,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewNode(tt.opts); err == nil {
				t.Error("NewNode accepted invalid options")
			}
		})
	}
}

func TestNewNodeAppliesDefaults(t *testing.T) {
	n, err := NewNode(Options{ID: 1, Config: NewConfig(1, 2, 3)})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.opts.ElectionTimeoutTicks <= n.opts.HeartbeatTimeoutTicks {
		t.Error("default timeouts do not leave room for heartbeats")
	}
	if !n.preVoteEnabled() {
		t.Error("pre-vote should default to enabled")
	}
	if !n.checkQuorumEnabled() {
		t.Error("check-quorum should default to enabled")
	}
	if n.opts.MaxEntriesPerMessage <= 0 || n.opts.MaxCommittedPerReady <= 0 {
		t.Error("batch limits were not defaulted")
	}
}

// --- helpers ---------------------------------------------------------------

// newTestNode builds a bare node for direct state-machine assertions, with a
// deterministic timeout draw so tests never depend on ambient randomness.
func newTestNode(t testing.TB, id NodeID, cfg Config) *Node {
	t.Helper()
	n, err := NewNode(Options{
		ID:                    id,
		Config:                cfg,
		ElectionTimeoutTicks:  10,
		HeartbeatTimeoutTicks: 1,
		Rand:                  func(int) int { return 0 },
	})
	if err != nil {
		t.Fatalf("NewNode(%d): %v", id, err)
	}
	return n
}

// lastMessage returns the most recent queued message of the given type.
func lastMessage(t testing.TB, n *Node, typ MessageType) Message {
	t.Helper()
	for i := len(n.msgs) - 1; i >= 0; i-- {
		if n.msgs[i].Type == typ {
			return n.msgs[i]
		}
	}
	t.Fatalf("no %s message was sent (queue: %v)", typ, n.msgs)
	return Message{}
}
