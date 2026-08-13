package raft

import (
	"fmt"
	"math/rand/v2"
)

// readIndexRequest is one linearizable read awaiting proof that the leader
// still leads.
type readIndexRequest struct {
	// index is the commit index the read must observe, captured when the
	// request arrived rather than when it is released — a later commit is not
	// something this read is required to see.
	index Index
	ctx   []byte
	// from is the node that asked, which may be this leader itself or a
	// follower that forwarded a client's read.
	from NodeID
	acks map[NodeID]bool
}

// SnapshotProvider supplies a snapshot of the state machine, used to catch up a
// follower that has fallen behind the leader's compaction boundary.
//
// It is an interface rather than a stored snapshot because a snapshot must be
// taken at the moment it is needed: the boundary moves as compaction proceeds,
// and a stale snapshot would be rejected by the follower it was meant to help.
type SnapshotProvider interface {
	Snapshot() (Snapshot, error)
}

// Options configures a node. Zero values are replaced by the defaults noted on
// each field, so the only mandatory settings are ID and Config.
type Options struct {
	// ID is this node's identifier. It must be non-zero and stable across
	// restarts: the identity is what a persisted vote refers to.
	ID NodeID

	// Config is the initial cluster membership. After a restart it is
	// overwritten by whatever the log and snapshot replay produce.
	Config Config

	// ElectionTimeoutTicks is the baseline number of ticks a follower waits
	// without hearing from a leader before standing for election. The actual
	// timeout is drawn from [t, 2t) per election. Defaults to 10.
	//
	// Randomisation is not a tuning detail — it is what makes elections
	// terminate. With identical timeouts, nodes campaign simultaneously, split
	// the vote, and repeat indefinitely.
	ElectionTimeoutTicks int

	// HeartbeatTimeoutTicks is how often a leader sends heartbeats. It must be
	// comfortably below ElectionTimeoutTicks — a ratio of about 10:1 — so that
	// ordinary network jitter cannot be mistaken for leader failure. Defaults
	// to 1.
	HeartbeatTimeoutTicks int

	// MaxEntriesPerMessage caps entries per AppendEntries, bounding message
	// size so that a far-behind follower is caught up in several messages
	// rather than one that exceeds the transport's frame limit. Defaults to 64.
	MaxEntriesPerMessage int

	// MaxCommittedPerReady caps entries handed to the state machine per Ready
	// batch, so a large backlog cannot block the node in a single apply.
	// Defaults to 256.
	MaxCommittedPerReady int

	// PreVote enables the §9.6 straw poll before a real election. Recommended:
	// without it, a node returning from a partition forces an unnecessary
	// leadership change. Defaults to enabled.
	PreVote *bool

	// CheckQuorum makes a leader step down when it cannot reach a majority
	// within an election timeout. Recommended: without it, a partitioned leader
	// keeps believing it leads and will serve stale reads. Defaults to enabled.
	CheckQuorum *bool

	// Rand draws a value in [0, n) for election-timeout randomisation. Supply a
	// seeded source to make a whole cluster's timing reproducible. Defaults to
	// the global source.
	Rand func(n int) int

	// Snapshots provides snapshots for catching up lagging followers. When nil,
	// a follower that falls behind the compaction boundary cannot be repaired
	// and replication to it stalls.
	Snapshots SnapshotProvider

	// Applied is the index the state machine has already consumed, set on
	// restart so that entries are not re-applied.
	Applied Index
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.ElectionTimeoutTicks <= 0 {
		out.ElectionTimeoutTicks = 10
	}
	if out.HeartbeatTimeoutTicks <= 0 {
		out.HeartbeatTimeoutTicks = 1
	}
	if out.MaxEntriesPerMessage <= 0 {
		out.MaxEntriesPerMessage = 64
	}
	if out.MaxCommittedPerReady <= 0 {
		out.MaxCommittedPerReady = 256
	}
	if out.PreVote == nil {
		out.PreVote = boolPtr(true)
	}
	if out.CheckQuorum == nil {
		out.CheckQuorum = boolPtr(true)
	}
	if out.Rand == nil {
		out.Rand = rand.IntN
	}
	return out
}

func boolPtr(b bool) *bool { return &b }

func (o Options) validate() error {
	if o.ID == None {
		return fmt.Errorf("raft: node id must be non-zero")
	}
	if err := o.Config.Validate(); err != nil {
		return err
	}
	if o.HeartbeatTimeoutTicks >= o.ElectionTimeoutTicks {
		return fmt.Errorf("raft: heartbeat timeout (%d) must be below election timeout (%d)",
			o.HeartbeatTimeoutTicks, o.ElectionTimeoutTicks)
	}
	return nil
}

// Node is a single Raft replica: a deterministic state machine driven by Step
// and Tick, producing work through Ready.
//
// It is not safe for concurrent use. Serialisation is the driver's job, and
// keeping it out of here is deliberate — a lock inside the core would make the
// simulator's interleavings depend on the Go scheduler rather than on its seed.
type Node struct {
	opts Options
	id   NodeID

	// --- durable state (§5.1): losing any of it can break safety ---
	term Term
	vote NodeID

	// --- volatile state ---
	role Role
	lead NodeID
	log  *raftLog
	cfg  Config

	// progress is leader-only replication state, rebuilt on every election.
	progress map[NodeID]*progress

	// votes accumulates responses during an election, including the pre-vote
	// straw poll. Reset on entering each campaign.
	votes map[NodeID]bool

	// electionElapsed counts ticks since the last useful contact: for a
	// follower, since hearing from a leader; for a leader, since its last
	// quorum self-check.
	electionElapsed  int
	heartbeatElapsed int
	// randomizedElectionTimeout is redrawn on every role change, so two nodes
	// that time out together do not keep colliding.
	randomizedElectionTimeout int

	// msgs is the outbound queue drained by Ready.
	msgs []Message

	// pendingReadStates holds linearizable reads whose index has been
	// confirmed and which are safe to serve once applied.
	pendingReadStates []ReadState

	// readOnly is the leader's queue of reads awaiting quorum confirmation, in
	// arrival order. Order matters: confirming one read retroactively confirms
	// every earlier one, so the queue is released as a prefix.
	readOnly []*readIndexRequest

	// pendingSnapshot is a snapshot received from a leader that the driver must
	// persist and hand to the state machine.
	pendingSnapshot *Snapshot

	// prevHardState and prevSoftState let Ready report only what changed,
	// so an idle node produces empty batches and no disk writes.
	prevHardState HardState
	prevSoftState SoftState
}

// NewNode creates a node in the follower state. A restarting node should follow
// this with Restore and ReplayEntries to rebuild state from durable storage
// before being stepped.
func NewNode(opts Options) (*Node, error) {
	o := opts.withDefaults()
	if err := o.validate(); err != nil {
		return nil, err
	}
	n := &Node{
		opts:     o,
		id:       o.ID,
		role:     Follower,
		lead:     None,
		log:      newLog(0, 0),
		cfg:      o.Config.Clone(),
		progress: make(map[NodeID]*progress),
		votes:    make(map[NodeID]bool),
	}
	if o.Applied > 0 {
		n.log.appliedTo(o.Applied)
	}
	n.resetProgress()
	n.resetRandomizedElectionTimeout()
	n.prevSoftState = n.softState()
	n.prevHardState = n.hardState()
	return n, nil
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// Role returns the node's current role.
func (n *Node) Role() Role { return n.role }

// Term returns the node's current term.
func (n *Node) Term() Term { return n.term }

// Leader returns the leader this node believes is in charge, or None.
func (n *Node) Leader() NodeID { return n.lead }

// CommitIndex returns the highest index known to be committed.
func (n *Node) CommitIndex() Index { return n.log.committed }

// AppliedIndex returns the highest index handed to the state machine.
func (n *Node) AppliedIndex() Index { return n.log.applied }

// LastIndex returns the highest index in the log.
func (n *Node) LastIndex() Index { return n.log.lastIndex() }

// SnapshotIndex returns the current compaction boundary.
func (n *Node) SnapshotIndex() Index { return n.log.snapIndex }

// Membership returns a copy of the current configuration.
func (n *Node) Membership() Config { return n.cfg.Clone() }

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
}

func (n *Node) softState() SoftState {
	return SoftState{Leader: n.lead, Role: n.role}
}

// preVoteEnabled and checkQuorumEnabled read the tri-state options.
func (n *Node) preVoteEnabled() bool     { return *n.opts.PreVote }
func (n *Node) checkQuorumEnabled() bool { return *n.opts.CheckQuorum }

// ---------------------------------------------------------------------------
// Durable state recovery
// ---------------------------------------------------------------------------

// SetHardState restores persisted term, vote and commit index. It must be
// called before the node is stepped, and never afterwards.
func (n *Node) SetHardState(hs HardState) {
	n.term = hs.Term
	n.vote = hs.Vote
	if hs.Commit > n.log.committed {
		// The commit index is only restored, never invented: a value above the
		// replayed log would mean committing entries this node does not hold.
		if hs.Commit <= n.log.lastIndex() {
			n.log.commitTo(hs.Commit)
		} else {
			n.log.commitTo(n.log.lastIndex())
		}
	}
	n.prevHardState = n.hardState()
}

// ReplayEntries appends entries recovered from the write-ahead log.
func (n *Node) ReplayEntries(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	n.log.append(ents...)
	// Replayed entries are by definition already durable, so they must not be
	// re-reported by Ready for persistence.
	n.log.stableTo(ents[len(ents)-1].Index)
	n.resetProgress()
}

// RestoreSnapshot re-establishes state from a persisted snapshot, adopting the
// membership it carries.
func (n *Node) RestoreSnapshot(snap Snapshot) error {
	if err := n.log.restore(snap); err != nil {
		return err
	}
	n.cfg = snap.Conf.Clone()
	n.resetProgress()
	n.prevHardState = n.hardState()
	return nil
}

// ---------------------------------------------------------------------------
// Driving the node
// ---------------------------------------------------------------------------

// Tick advances the logical clock by one unit, which may trigger an election,
// a heartbeat round, or a leader's quorum self-check.
func (n *Node) Tick() {
	switch n.role {
	case Leader:
		n.tickLeader()
	default:
		n.tickFollower()
	}
}

func (n *Node) tickLeader() {
	n.heartbeatElapsed++
	n.electionElapsed++

	if n.electionElapsed >= n.opts.ElectionTimeoutTicks {
		n.electionElapsed = 0
		if n.checkQuorumEnabled() {
			n.checkQuorumOrStepDown()
		}
	}
	// The quorum check may have deposed this node, so re-test the role before
	// broadcasting as leader.
	if n.role != Leader {
		return
	}
	if n.heartbeatElapsed >= n.opts.HeartbeatTimeoutTicks {
		n.heartbeatElapsed = 0
		n.bcastHeartbeat()
	}
}

func (n *Node) tickFollower() {
	if !n.promotable() {
		// Learners and nodes not in the configuration never campaign. A learner
		// that stood for election could win with votes it is not entitled to
		// cast, defeating the point of non-voting membership.
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randomizedElectionTimeout {
		n.electionElapsed = 0
		n.campaign()
	}
}

// promotable reports whether this node may stand for election.
func (n *Node) promotable() bool {
	return n.cfg.IsVoter(n.id)
}

// checkQuorumOrStepDown deposes this leader if a majority has not been heard
// from within the last election timeout.
//
// Without this, a leader isolated by a partition continues to believe it leads.
// It cannot commit anything — commitment needs a majority — but it would
// happily answer reads from its own state, which the rest of the cluster has
// already moved past under a new leader. Stepping down converts a silent
// correctness violation into a visible unavailability.
func (n *Node) checkQuorumOrStepDown() {
	active := make(map[NodeID]bool, len(n.progress))
	for id, pr := range n.progress {
		if id == n.id {
			// A leader is trivially in contact with itself.
			active[id] = true
			continue
		}
		active[id] = pr.RecentActive
		// Reset for the next interval: liveness must be re-proved each time,
		// not remembered forever.
		pr.RecentActive = false
	}
	if !n.cfg.HasQuorum(active) {
		n.becomeFollower(n.term, None)
	}
}

// Campaign forces this node to stand for election immediately, skipping the
// election timeout. It is used for leadership transfer and by tests; ordinary
// elections are triggered by Tick.
func (n *Node) Campaign() {
	if !n.promotable() {
		return
	}
	n.campaignAt(campaignElection)
}

type campaignType uint8

const (
	// campaignPreElection is the straw poll: no term change, no persisted vote.
	campaignPreElection campaignType = iota
	// campaignElection is a real election: term incremented, vote persisted.
	campaignElection
)

func (n *Node) campaign() {
	if n.preVoteEnabled() {
		n.campaignAt(campaignPreElection)
		return
	}
	n.campaignAt(campaignElection)
}

func (n *Node) campaignAt(t campaignType) {
	var voteTerm Term
	if t == campaignPreElection {
		n.becomePreCandidate()
		// The straw poll asks about the term this node *would* move to, while
		// its own term stays put. Only a successful poll justifies the increment.
		voteTerm = n.term + 1
	} else {
		n.becomeCandidate()
		voteTerm = n.term
	}

	// A single-voter cluster reaches quorum on its own vote, with no messages.
	if won, decided := n.poll(n.id, true); decided && won {
		n.electionWon(t)
		return
	}

	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	for _, id := range n.cfg.VoterIDs() {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVoteReq,
			To:           id,
			Term:         voteTerm,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
			PreVote:      t == campaignPreElection,
		})
	}
}

// electionWon advances a successful straw poll into a real election, or a
// successful election into leadership.
func (n *Node) electionWon(t campaignType) {
	if t == campaignPreElection {
		n.campaignAt(campaignElection)
		return
	}
	n.becomeLeader()
}

// poll records a vote and reports whether the election is decided, and if so
// whether it was won. An election is lost as soon as a majority is impossible,
// which lets a node fall back to follower without waiting for stragglers.
func (n *Node) poll(from NodeID, granted bool) (won bool, decided bool) {
	if _, seen := n.votes[from]; !seen {
		n.votes[from] = granted
	}
	if n.cfg.HasQuorum(n.votes) {
		return true, true
	}
	// Count rejections against the possibility of ever reaching quorum: if the
	// nodes that have refused already exclude a majority, the election is lost.
	rejected := make(map[NodeID]bool, len(n.votes))
	for id, g := range n.votes {
		rejected[id] = !g
	}
	if n.cfg.HasQuorum(rejected) {
		return false, true
	}
	return false, false
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

// reset prepares for a new term: clears the vote if the term advanced, drops
// leader-only state, and redraws the election timeout.
func (n *Node) reset(term Term) {
	if n.term != term {
		n.term = term
		// A vote belongs to the term it was cast in. Carrying it forward would
		// silently disenfranchise this node in the new term.
		n.vote = None
	}
	n.lead = None
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.resetRandomizedElectionTimeout()
	n.votes = make(map[NodeID]bool)
	n.resetProgress()
}

func (n *Node) resetProgress() {
	next := n.log.lastIndex() + 1
	fresh := make(map[NodeID]*progress, len(n.cfg.Members()))
	for _, id := range n.cfg.Members() {
		if existing, ok := n.progress[id]; ok {
			existing.IsLearner = n.cfg.IsLearner(id)
			fresh[id] = existing
			continue
		}
		fresh[id] = newProgress(next, n.cfg.IsLearner(id))
	}
	n.progress = fresh
}

func (n *Node) resetRandomizedElectionTimeout() {
	t := n.opts.ElectionTimeoutTicks
	n.randomizedElectionTimeout = t + n.opts.Rand(t)
}

func (n *Node) becomeFollower(term Term, lead NodeID) {
	wasLeader := n.role == Leader
	n.reset(term)
	n.role = Follower
	n.lead = lead
	if wasLeader {
		// Reads this node had promised can no longer be proven linearizable, so
		// they are refused rather than answered from state the cluster may have
		// moved past. Refusing after the term has been updated lets the reply
		// reach a follower that forwarded the read, so a client learns at once
		// instead of waiting out a timeout.
		n.dropPendingReads()
	}
}

func (n *Node) becomePreCandidate() {
	if n.role == Leader {
		panic("raft: a leader may not become a pre-candidate")
	}
	// Deliberately no reset: the term must not change and the persisted vote
	// must not be touched. Only the poll is cleared.
	n.role = PreCandidate
	n.lead = None
	n.votes = make(map[NodeID]bool)
	n.resetRandomizedElectionTimeout()
}

func (n *Node) becomeCandidate() {
	if n.role == Leader {
		panic("raft: a leader may not become a candidate")
	}
	n.reset(n.term + 1)
	n.role = Candidate
	// A candidate votes for itself, and that vote is durable state: forgetting
	// it across a crash would let this node vote again in the same term.
	n.vote = n.id
}

func (n *Node) becomeLeader() {
	if n.role == Leader {
		return
	}
	if n.role == Follower {
		panic("raft: a follower may not become leader without an election")
	}
	term := n.term
	n.reset(term)
	n.role = Leader
	n.lead = n.id

	for id, pr := range n.progress {
		if id == n.id {
			// The leader's own log is trivially replicated to itself, but only
			// up to what is durable. Match advances the rest of the way in
			// Advance, once the driver confirms the fsync.
			pr.Match = n.log.stable
			pr.becomeReplicate()
			pr.RecentActive = true
		} else {
			// Match resets to zero because a new leader knows nothing about any
			// follower's log. Carrying over a Match from a previous leadership
			// would be a safety hazard: the value could be stale, and the commit
			// index is computed from it.
			pr.Match = 0
			pr.becomeProbe()
			pr.RecentActive = false
		}
		// Probe from the end of the log and walk back on rejection. Starting at
		// the optimistic position costs one round trip when the follower is
		// caught up, which is the common case.
		pr.Next = n.log.lastIndex() + 1
	}

	// Commit a blank entry of the new term (§5.4.2). A leader may not conclude
	// that an entry from an earlier term is committed merely because it is now
	// on a majority — Figure 8 of the paper shows such an entry can still be
	// overwritten. Committing an entry of the *current* term commits everything
	// before it transitively and safely. It also establishes the commit index
	// promptly, so linearizable reads are available without waiting for the
	// first client write.
	n.log.append(Entry{Term: term, Index: n.log.lastIndex() + 1, Type: EntryNoOp})
	n.bcastAppend()
}

// ---------------------------------------------------------------------------
// Proposals
// ---------------------------------------------------------------------------

// Propose appends a client command to the log and starts replicating it,
// returning the index it will occupy once committed.
//
// The returned index is a promise about position, not about outcome: the entry
// is committed only if a majority accepts it, and a leader deposed before then
// will have it overwritten. Callers must wait for the entry to be applied
// before reporting success to a client.
func (n *Node) Propose(data []byte) (Index, error) {
	return n.appendEntry(EntryNormal, data)
}

// ProposeConfChange appends a membership change to the log.
func (n *Node) ProposeConfChange(data []byte) (Index, error) {
	return n.appendEntry(EntryConfChange, data)
}

func (n *Node) appendEntry(t EntryType, data []byte) (Index, error) {
	if n.role != Leader {
		return 0, ErrProposalDropped
	}
	// A leader that has been removed from the configuration must stop accepting
	// proposals: it can no longer be part of any quorum that would commit them.
	if !n.cfg.Contains(n.id) {
		return 0, ErrProposalDropped
	}
	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: t, Data: data}
	n.log.append(e)
	n.bcastAppend()
	return e.Index, nil
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

// Step delivers a message to the node. It returns an error only for a malformed
// message; a stale or otherwise unwanted message is handled by the protocol and
// reported as success, since dropping it is the correct behaviour rather than a
// failure of the caller.
func (n *Node) Step(m Message) error {
	if m.Term == 0 {
		return fmt.Errorf("raft: message %s from %d has no term", m.Type, m.From)
	}

	switch {
	case m.Term > n.term:
		if handled := n.stepHigherTerm(m); handled {
			return nil
		}
	case m.Term < n.term:
		n.stepLowerTerm(m)
		return nil
	}

	switch n.role {
	case Leader:
		n.stepLeader(m)
	case Candidate, PreCandidate:
		n.stepCandidate(m)
	default:
		n.stepFollower(m)
	}
	return nil
}

// stepHigherTerm handles a message from a higher term. It reports true if the
// message was fully consumed here and needs no further dispatch.
func (n *Node) stepHigherTerm(m Message) bool {
	switch {
	case m.Type == MsgVoteReq && m.PreVote:
		// A pre-vote request must not move this node's term or depose its
		// leader. That restraint is the entire mechanism: a partitioned node
		// polling with an inflated term learns it cannot win without anyone
		// else being disturbed. Fall through to evaluate the poll.
		n.handleVoteRequest(m)
		return true

	case m.Type == MsgVoteResp && m.PreVote && m.Granted:
		// Granted pre-votes carry the term this node *would* move to, which is
		// necessarily above its own. Adopting it here would defeat the purpose;
		// the term is incremented only when the poll succeeds.
		return false

	case m.Type == MsgVoteReq:
		// A real vote request reveals a newer term but says nothing about who
		// leads it, so no leader is recorded.
		n.becomeFollower(m.Term, None)

	case m.Type == MsgAppReq || m.Type == MsgSnapReq || m.Type == MsgTimeoutNow:
		// These come from a leader, so its identity is known immediately and
		// clients can be redirected without waiting for a heartbeat.
		n.becomeFollower(m.Term, m.From)

	default:
		n.becomeFollower(m.Term, None)
	}
	return false
}

// stepLowerTerm handles a message from a stale term.
func (n *Node) stepLowerTerm(m Message) {
	switch m.Type {
	case MsgAppReq, MsgSnapReq:
		// Answer a deposed leader with the current term so it steps down at
		// once. Silence would leave it retrying until its own election timeout,
		// during which clients pointed at it see failures.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true})

	case MsgVoteReq:
		// Tell a stale candidate the real term. For a pre-vote this is exactly
		// how the mechanism helps a returning node: it learns the current term
		// and rejoins as a follower instead of forcing an election.
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Granted: false, PreVote: m.PreVote})

	default:
		// Stale responses carry no useful information and are dropped.
	}
}

func (n *Node) stepFollower(m Message) {
	switch m.Type {
	case MsgAppReq:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleAppendEntries(m)

	case MsgSnapReq:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleSnapshot(m)

	case MsgVoteReq:
		n.handleVoteRequest(m)

	case MsgTimeoutNow:
		// Leadership transfer: campaign at once, skipping the pre-vote phase.
		// The straw poll would be pointless here — the sitting leader has
		// already decided to yield.
		if n.promotable() {
			n.campaignAt(campaignElection)
		}

	case MsgReadIndexReq:
		// A follower cannot answer this: only the leader knows the commit index
		// that a linearizable read must observe. Forward it, or refuse if there
		// is no leader to forward to.
		if n.lead == None {
			n.send(Message{Type: MsgReadIndexResp, To: m.From, Term: n.term, Reject: true, ReadCtx: m.ReadCtx})
			return
		}
		fwd := m
		fwd.To = n.lead
		fwd.From = m.From
		n.send(fwd)

	case MsgReadIndexResp:
		n.pendingReadStates = append(n.pendingReadStates, ReadState{Index: m.ReadIndex, ReadCtx: m.ReadCtx})
	}
}

func (n *Node) stepCandidate(m Message) {
	// A candidate expects responses matching its own campaign phase. A pre-vote
	// response arriving during a real election (or the reverse) is a leftover
	// from the previous phase and must be ignored, or a stale straw poll could
	// be counted toward a real election.
	switch m.Type {
	case MsgVoteResp:
		if m.PreVote != (n.role == PreCandidate) {
			return
		}
		won, decided := n.poll(m.From, m.Granted)
		if !decided {
			return
		}
		if won {
			if n.role == PreCandidate {
				n.electionWon(campaignPreElection)
			} else {
				n.electionWon(campaignElection)
			}
			return
		}
		// A majority refused. Step down and wait out another timeout rather
		// than campaigning again immediately.
		n.becomeFollower(n.term, None)

	case MsgAppReq:
		// A legitimate leader exists for this term: concede.
		n.becomeFollower(m.Term, m.From)
		n.handleAppendEntries(m)

	case MsgSnapReq:
		n.becomeFollower(m.Term, m.From)
		n.handleSnapshot(m)

	case MsgVoteReq:
		// Having voted for itself this term, a candidate refuses everyone else.
		n.handleVoteRequest(m)
	}
}

func (n *Node) stepLeader(m Message) {
	pr := n.progress[m.From]

	switch m.Type {
	case MsgAppResp:
		if pr == nil {
			return
		}
		pr.RecentActive = true
		if len(m.ReadCtx) > 0 {
			// This reply echoes a read context, so it is proof that the peer
			// still recognised this leader after the read was recorded.
			n.recvReadIndexAck(m.From, m.ReadCtx)
		}
		if m.Reject {
			n.handleAppendRejection(m, pr)
			return
		}
		if pr.maybeUpdate(m.MatchIndex) {
			if pr.State == stateProbe {
				// The follower's log is located; switch to streaming.
				pr.becomeReplicate()
			}
			if n.maybeCommit() {
				// Tell followers about the new commit index promptly, so their
				// state machines advance without waiting for a heartbeat.
				n.bcastAppend()
				return
			}
		}
		// Keep feeding a follower that still has entries owing. This is checked
		// even when Match did not advance, because a heartbeat acknowledgement
		// carries no new index yet still tells the leader the peer is reachable
		// and may have entries outstanding.
		if pr.Next <= n.log.lastIndex() {
			n.sendAppend(m.From)
		}

	case MsgSnapResp:
		if pr == nil {
			return
		}
		pr.RecentActive = true
		if m.Reject {
			// The follower refused the snapshot; fall back to probing its log.
			pr.becomeProbe()
			return
		}
		pr.maybeUpdate(m.MatchIndex)
		pr.becomeProbe()
		n.sendAppend(m.From)

	case MsgVoteReq:
		// Same term as this leader's, so this is a node that never learned about
		// the election. Refuse: this node has already voted for itself.
		n.handleVoteRequest(m)

	case MsgReadIndexReq:
		n.handleReadIndex(m)
	}
}

func (n *Node) handleAppendRejection(m Message, pr *progress) {
	// The follower reported where its own log diverges. The leader now walks
	// back over its *own* entries whose term exceeds the follower's at that
	// point, because none of them can possibly match.
	//
	// Both halves of this walk are needed. The follower's hint skips its
	// conflicting suffix; the leader's walk skips the run of its own entries
	// that could never match it. Together they cost one round trip per
	// conflicting term. Either half alone degrades to one round trip per
	// conflicting entry, which after a long partition is thousands.
	nextProbe := m.HintIndex
	if m.HintTerm > 0 {
		nextProbe, _ = n.log.findConflictByTerm(m.HintIndex, m.HintTerm)
	}

	// MatchIndex on a rejection carries the index that was refused.
	if !pr.maybeDecrTo(m.MatchIndex, nextProbe) {
		// A stale or duplicated rejection: acting on it would walk Next
		// backwards for no reason and slow the repair down.
		return
	}
	if pr.State == stateReplicate {
		pr.becomeProbe()
	}
	n.sendAppend(m.From)
}

// handleVoteRequest decides a vote (real or straw poll) and replies.
func (n *Node) handleVoteRequest(m Message) {
	// A vote may be granted when this node has not yet voted this term and
	// knows of no leader, or when the same candidate is asking again (making
	// retries idempotent), or for a pre-vote about a future term, which commits
	// this node to nothing.
	canVote := n.vote == m.From ||
		(n.vote == None && n.lead == None) ||
		(m.PreVote && m.Term > n.term)

	// The §5.4.1 election restriction: never vote for a candidate whose log is
	// behind this node's. This is what guarantees a new leader holds every
	// committed entry.
	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)
	granted := canVote && upToDate

	// A pre-vote reply is addressed to the term the candidate asked about, so
	// that it can tell a granted poll from a rejection carrying a real term.
	replyTerm := n.term
	if m.PreVote {
		replyTerm = m.Term
	}
	n.send(Message{
		Type:    MsgVoteResp,
		To:      m.From,
		Term:    replyTerm,
		Granted: granted,
		PreVote: m.PreVote,
	})

	if granted && !m.PreVote {
		// Record the real vote and treat the request as contact from a live
		// candidate, so this node does not immediately campaign against it.
		n.vote = m.From
		n.electionElapsed = 0
	}
}

// handleAppendEntries is the follower side of replication.
func (n *Node) handleAppendEntries(m Message) {
	// Every response echoes the read context, if any. A follower's reply is what
	// proves to the leader that it was still recognised at that moment, and that
	// is true whether the append itself succeeded or was rejected.
	if m.PrevLogIndex < n.log.committed {
		// The leader is probing below this node's commit index. Everything up to
		// the commit index is settled and cannot conflict, so instead of letting
		// the leader keep walking backwards, answer with the commit index and
		// let it jump straight there.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, MatchIndex: n.log.committed, ReadCtx: m.ReadCtx})
		return
	}

	if lastNew, ok := n.log.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.Commit, m.Entries); ok {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, MatchIndex: lastNew, ReadCtx: m.ReadCtx})
		return
	}

	// The consistency check failed. Report where this node's log actually
	// diverges so the leader can skip the whole conflicting term in one step
	// rather than decrementing an index per round trip.
	hintIndex, hintTerm := n.log.findConflictByTerm(min(m.PrevLogIndex, n.log.lastIndex()), m.PrevLogTerm)
	n.send(Message{
		Type:       MsgAppResp,
		To:         m.From,
		Term:       n.term,
		Reject:     true,
		MatchIndex: m.PrevLogIndex,
		HintIndex:  hintIndex,
		HintTerm:   hintTerm,
		ReadCtx:    m.ReadCtx,
	})
}

// handleSnapshot restores state from a leader's snapshot.
func (n *Node) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	snap := *m.Snapshot
	if err := n.log.restore(snap); err != nil {
		// The snapshot adds nothing this node does not already have. Report the
		// current commit index so the leader can resume from the right place.
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, MatchIndex: n.log.committed})
		return
	}
	n.cfg = snap.Conf.Clone()
	n.resetProgress()
	// The driver must persist this and hand it to the state machine; it is
	// surfaced through Ready rather than applied behind the caller's back.
	n.pendingSnapshot = &snap
	n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, MatchIndex: snap.Index})
}

// ReadIndex requests a linearizable read. The returned context is echoed in the
// ReadState that Ready delivers once the read is safe to serve.
func (n *Node) ReadIndex(ctx []byte) {
	if n.role == Leader {
		n.handleReadIndex(Message{Type: MsgReadIndexReq, From: n.id, Term: n.term, ReadCtx: ctx})
		return
	}
	if n.lead == None {
		n.pendingReadStates = append(n.pendingReadStates, ReadState{Index: 0, ReadCtx: ctx})
		return
	}
	n.send(Message{Type: MsgReadIndexReq, To: n.lead, From: n.id, Term: n.term, ReadCtx: ctx})
}

// handleReadIndex resolves a linearizable read on the leader (§6.4).
//
// The naive alternative — the leader answering from its own applied state — is
// not linearizable. A leader partitioned away from the cluster does not learn
// that it has been deposed until its own timeout expires, and in the meantime it
// would serve values a new leader has already overwritten.
//
// So the leader records its current commit index, then proves it still leads by
// collecting heartbeat acknowledgements from a majority. Only then is the read
// released. This costs one round trip and, crucially, no log append: reads do
// not grow the log or contend with writes for the commit pipeline.
func (n *Node) handleReadIndex(m Message) {
	// A leader that has not yet committed an entry of its own term does not know
	// the true commit index — an entry from a previous term may still be
	// overwritten (§5.4.2). Refuse rather than bound the read by an index that
	// could regress. The no-op committed on election clears this within one
	// round trip.
	if !n.committedInCurrentTerm() {
		n.send(Message{Type: MsgReadIndexResp, To: m.From, Term: n.term, Reject: true, ReadCtx: m.ReadCtx})
		return
	}

	// In a single-voter cluster the leader is the entire quorum, so leadership
	// needs no confirmation from anyone else.
	if len(n.cfg.VoterIDs()) == 1 {
		n.deliverReadIndex(m.From, m.ReadCtx, n.log.committed)
		return
	}

	req := &readIndexRequest{
		index: n.log.committed,
		ctx:   append([]byte(nil), m.ReadCtx...),
		from:  m.From,
		acks:  map[NodeID]bool{n.id: true},
	}
	n.readOnly = append(n.readOnly, req)
	n.bcastHeartbeatWithCtx(req.ctx)
}

// recvReadIndexAck records a heartbeat acknowledgement carrying a read context
// and releases every read that leadership is now proven for.
func (n *Node) recvReadIndexAck(from NodeID, ctx []byte) {
	idx := -1
	for i, req := range n.readOnly {
		if string(req.ctx) == string(ctx) {
			req.acks[from] = true
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	if !n.cfg.HasQuorum(n.readOnly[idx].acks) {
		return
	}
	// A majority has confirmed this leader at this moment, which retroactively
	// proves leadership for every read recorded earlier too: each has an index
	// no higher than this one, and this acknowledgement is more recent than all
	// of them. Release the whole prefix.
	for _, req := range n.readOnly[:idx+1] {
		n.deliverReadIndex(req.from, req.ctx, req.index)
	}
	n.readOnly = n.readOnly[idx+1:]
}

// dropPendingReads refuses every outstanding read. It runs whenever this node
// stops being the leader of its term: those reads can no longer be proven
// linearizable, and answering them anyway is exactly the stale read the
// mechanism exists to prevent.
func (n *Node) dropPendingReads() {
	for _, req := range n.readOnly {
		if req.from == n.id {
			// A local caller learns of the failure through a zero index, which
			// the driver reports as "not leader" so the client can retry.
			n.pendingReadStates = append(n.pendingReadStates, ReadState{Index: 0, ReadCtx: req.ctx})
			continue
		}
		n.send(Message{Type: MsgReadIndexResp, To: req.from, Term: n.term, Reject: true, ReadCtx: req.ctx})
	}
	n.readOnly = nil
}

func (n *Node) deliverReadIndex(to NodeID, ctx []byte, readIndex Index) {
	if to == n.id {
		n.pendingReadStates = append(n.pendingReadStates, ReadState{Index: readIndex, ReadCtx: ctx})
		return
	}
	n.send(Message{Type: MsgReadIndexResp, To: to, Term: n.term, ReadIndex: readIndex, ReadCtx: ctx})
}

// committedInCurrentTerm reports whether the leader has committed at least one
// entry of its own term, which is the precondition for trusting its commit
// index (§5.4.2).
func (n *Node) committedInCurrentTerm() bool {
	t, err := n.log.term(n.log.committed)
	return err == nil && t == n.term
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

func (n *Node) bcastAppend() {
	for _, id := range n.cfg.Members() {
		if id == n.id {
			continue
		}
		n.sendAppend(id)
	}
}

// bcastHeartbeat sends a heartbeat to every peer and clears outstanding probe
// suppression, so a probe lost in the network is retried each interval rather
// than stalling replication to that peer indefinitely.
func (n *Node) bcastHeartbeat() { n.bcastHeartbeatWithCtx(nil) }

// bcastHeartbeatWithCtx sends heartbeats carrying a read context, which peers
// echo back so their replies double as proof of leadership for that read.
func (n *Node) bcastHeartbeatWithCtx(ctx []byte) {
	for _, id := range n.cfg.Members() {
		if id == n.id {
			continue
		}
		pr := n.progress[id]
		if pr == nil {
			continue
		}
		// A probe may have been lost in the network. Clearing the suppression
		// each interval retries it, rather than stalling replication to this
		// peer until an election timeout.
		pr.ProbeSent = false
		n.sendHeartbeat(id, pr, ctx)
		n.nudgeIfStalled(id, pr)
	}
}

// nudgeIfStalled recovers a replication stream whose in-flight entries were
// silently dropped.
//
// A streaming peer advances Next optimistically on send, so if those messages
// are lost, the leader has nothing left to send and the peer never acknowledges:
// replication to it wedges until an election intervenes. The ambiguous state —
// everything sent, nothing acknowledged — is also what ordinary pipelining looks
// like, so it is only treated as loss after a couple of quiet intervals.
//
// Recovery deliberately rewinds to Match+1 rather than retrying at Next-1. The
// follower is known to hold Match, so the retry passes its consistency check on
// the first attempt and carries real entries with it. Retrying at Next-1 would
// anchor on an entry the follower may never have received, and the resulting
// rejection would drop a healthy stream into probing for no reason.
func (n *Node) nudgeIfStalled(to NodeID, pr *progress) {
	if pr.State != stateReplicate || pr.Match >= n.log.lastIndex() {
		pr.StalledIntervals = 0
		return
	}
	if pr.Next <= n.log.lastIndex() {
		// There are still entries this peer has never been sent, so the normal
		// path will deliver them; nothing is stuck.
		pr.StalledIntervals = 0
		n.sendAppend(to)
		return
	}
	pr.StalledIntervals++
	if pr.StalledIntervals < 2 {
		return
	}
	pr.StalledIntervals = 0
	pr.becomeProbe()
	n.sendAppend(to)
}

// sendHeartbeat sends an empty AppendEntries anchored at the follower's
// acknowledged match index.
//
// Anchoring at Match rather than at Next-1 is deliberate. While streaming, Next
// runs ahead of what the follower has acknowledged, so a heartbeat anchored
// there would fail the consistency check whenever entries are still in flight —
// turning every heartbeat under load into a spurious rejection that knocks
// replication back into probing. Match is by definition already present on the
// follower, so the check always passes and the heartbeat does its real job:
// proving leadership and carrying the commit index forward.
func (n *Node) sendHeartbeat(to NodeID, pr *progress, ctx []byte) {
	if pr.Match < n.log.snapIndex {
		// The follower is behind the compaction boundary, so there is no entry
		// to anchor against. It needs a snapshot, not a heartbeat.
		n.sendAppend(to)
		return
	}
	prevTerm, err := n.log.term(pr.Match)
	if err != nil {
		n.sendAppend(to)
		return
	}
	n.send(Message{
		Type:         MsgAppReq,
		To:           to,
		Term:         n.term,
		PrevLogIndex: pr.Match,
		PrevLogTerm:  prevTerm,
		// A follower may only commit what it actually holds, so cap the
		// advertised commit index at what this peer has acknowledged.
		Commit:  min(n.log.committed, pr.Match),
		ReadCtx: ctx,
	})
}

// sendAppend sends entries to a peer, or a snapshot if the peer has fallen
// behind the compaction boundary.
func (n *Node) sendAppend(to NodeID) {
	pr := n.progress[to]
	if pr == nil || pr.isPaused() {
		return
	}

	prevIndex := pr.Next - 1
	prevTerm, err := n.log.term(prevIndex)
	if err != nil {
		// The entry the leader would anchor against has been compacted away,
		// so no sequence of log entries can repair this follower.
		n.sendSnapshot(to, pr)
		return
	}

	ents, err := n.log.slice(pr.Next, n.log.lastIndex()+1, n.opts.MaxEntriesPerMessage)
	if err != nil {
		n.sendSnapshot(to, pr)
		return
	}

	n.send(Message{
		Type:         MsgAppReq,
		To:           to,
		Term:         n.term,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		Commit:       n.log.committed,
	})

	switch pr.State {
	case stateReplicate:
		// Optimistically advance Next so the next batch can be sent without
		// waiting for this one to be acknowledged. Match is untouched: only an
		// acknowledgement may advance that, since commitment depends on it.
		if len(ents) > 0 {
			pr.Next = ents[len(ents)-1].Index + 1
		}
	case stateProbe:
		// Exactly one probe at a time, so each rejection is interpreted against
		// a known outstanding message.
		pr.ProbeSent = true
	}
}

func (n *Node) sendSnapshot(to NodeID, pr *progress) {
	if n.opts.Snapshots == nil {
		// Without a snapshot source this follower cannot be repaired. Pausing
		// is the honest outcome: it keeps the leader from spinning on a probe
		// that can never succeed, and the stall is visible in peer status.
		pr.becomeSnapshot(n.log.snapIndex)
		return
	}
	snap, err := n.opts.Snapshots.Snapshot()
	if err != nil {
		return
	}
	pr.becomeSnapshot(snap.Index)
	n.send(Message{
		Type:     MsgSnapReq,
		To:       to,
		Term:     n.term,
		Snapshot: &snap,
	})
}

// maybeCommit advances the commit index if a majority has replicated further,
// reporting whether it moved.
func (n *Node) maybeCommit() bool {
	match := make(map[NodeID]Index, len(n.progress))
	for id, pr := range n.progress {
		if n.cfg.IsVoter(id) {
			match[id] = pr.Match
		}
	}
	candidate := n.cfg.CommittedIndex(match)
	if candidate <= n.log.committed {
		return false
	}
	// §5.4.2: an entry from an earlier term may not be committed by counting
	// replicas, because Figure 8 shows it can still be overwritten. The leader's
	// own no-op entry is what eventually lifts the commit index past this gate,
	// carrying the earlier entries with it.
	if t, err := n.log.term(candidate); err != nil || t != n.term {
		return false
	}
	n.log.commitTo(candidate)
	return true
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

// ---------------------------------------------------------------------------
// Ready / Advance
// ---------------------------------------------------------------------------

// HasReady reports whether there is work to do, letting a driver skip the cost
// of building a batch when the node is idle.
func (n *Node) HasReady() bool {
	return n.hardState() != n.prevHardState ||
		n.softState() != n.prevSoftState ||
		n.pendingSnapshot != nil ||
		len(n.msgs) > 0 ||
		len(n.pendingReadStates) > 0 ||
		len(n.log.unstableEntries()) > 0 ||
		n.log.hasPendingApply()
}

// Ready returns the work the driver must perform. The caller must honour the
// ordering documented on Ready — persist, then send, then apply — and then call
// Advance with the same batch.
//
// No Step or Tick call may be interleaved between Ready and Advance: the batch
// describes a snapshot of state, and mutating that state underneath it would
// acknowledge work that was never performed.
func (n *Node) Ready() Ready {
	rd := Ready{
		Entries:    n.log.unstableEntries(),
		Committed:  n.log.nextCommitted(n.opts.MaxCommittedPerReady),
		Messages:   n.msgs,
		ReadStates: n.pendingReadStates,
		Snapshot:   n.pendingSnapshot,
	}
	if hs := n.hardState(); hs != n.prevHardState {
		rd.HardState = &hs
	}
	if ss := n.softState(); ss != n.prevSoftState {
		rd.SoftState = &ss
	}
	return rd
}

// Advance acknowledges that the batch returned by Ready has been persisted,
// sent and applied.
func (n *Node) Advance(rd Ready) {
	if rd.HardState != nil {
		n.prevHardState = *rd.HardState
	}
	if rd.SoftState != nil {
		n.prevSoftState = *rd.SoftState
	}
	if rd.Snapshot != nil {
		n.pendingSnapshot = nil
	}

	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index
		n.log.stableTo(last)
		// The leader counts itself only once its own write is durable. Counting
		// on append instead would let it commit an entry on the strength of an
		// fsync that has not happened, and lose it in a crash.
		if n.role == Leader {
			if pr := n.progress[n.id]; pr != nil && pr.maybeUpdate(n.log.stable) {
				if n.maybeCommit() {
					n.bcastAppend()
				}
			}
		}
	}

	if len(rd.Committed) > 0 {
		n.log.appliedTo(rd.Committed[len(rd.Committed)-1].Index)
	}

	// Drop exactly what was reported. Handling a message during apply could
	// have queued more, and discarding those would lose them.
	n.msgs = n.msgs[len(rd.Messages):]
	n.pendingReadStates = n.pendingReadStates[len(rd.ReadStates):]
}

// Compact folds the log prefix through index i into the snapshot boundary,
// releasing the memory those entries occupied. Only applied entries may be
// compacted.
func (n *Node) Compact(i Index) error {
	return n.log.compact(i)
}

// Status is a point-in-time description of the node, for operators and metrics.
type Status struct {
	ID       NodeID
	Role     Role
	Term     Term
	Leader   NodeID
	Commit   Index
	Applied  Index
	LastLog  Index
	Snapshot Index
	Config   Config
	Progress map[NodeID]progress
}

// Status returns a snapshot of the node's state. Progress is populated only on
// a leader, since no other role tracks it.
func (n *Node) Status() Status {
	s := Status{
		ID:       n.id,
		Role:     n.role,
		Term:     n.term,
		Leader:   n.lead,
		Commit:   n.log.committed,
		Applied:  n.log.applied,
		LastLog:  n.log.lastIndex(),
		Snapshot: n.log.snapIndex,
		Config:   n.cfg.Clone(),
	}
	if n.role == Leader {
		s.Progress = make(map[NodeID]progress, len(n.progress))
		for id, pr := range n.progress {
			s.Progress[id] = *pr
		}
	}
	return s
}
