package raft

import "fmt"

// progressState is the leader's model of how to replicate to one follower.
//
// The distinction exists because a leader does not know a new follower's log.
// Discovering the divergence point requires probing, and probing must be
// throttled to one in-flight message at a time — otherwise the leader sends a
// burst of guesses, each rejected, and interprets the resulting flood of stale
// rejections as further divergence.
type progressState uint8

const (
	// stateProbe sends at most one AppendEntries at a time and waits for the
	// reply, used while the leader is still locating the follower's divergence
	// point.
	stateProbe progressState = iota
	// stateReplicate streams entries optimistically, advancing Next on send
	// without waiting for acknowledgement. This is the steady state and the
	// only one that achieves pipelined throughput.
	stateReplicate
	// stateSnapshot means a snapshot transfer is in flight. Log entries are not
	// sent, because the follower is behind the compaction boundary and no entry
	// the leader still holds would pass its consistency check.
	stateSnapshot
)

func (s progressState) String() string {
	switch s {
	case stateProbe:
		return "probe"
	case stateReplicate:
		return "replicate"
	case stateSnapshot:
		return "snapshot"
	default:
		return fmt.Sprintf("progressState(%d)", uint8(s))
	}
}

// progress tracks the leader's replication state for a single peer. It is
// leader-only, non-durable state: a new leader rebuilds it from scratch, which
// is why it may be optimistic without risking safety.
type progress struct {
	// Match is the highest log index known to be replicated on this peer. It is
	// advanced only by an acknowledgement, never by a send, because the commit
	// index is computed from it and commitment must never be based on a guess.
	Match Index
	// Next is the index of the next entry to send. Unlike Match it is
	// speculative: in stateReplicate it runs ahead of Match by the number of
	// in-flight entries.
	Next Index

	State progressState

	// PendingSnapshot is the snapshot index being transferred in stateSnapshot.
	PendingSnapshot Index

	// ProbeSent is set while a probe is outstanding, suppressing further sends
	// until the peer replies or a heartbeat interval elapses.
	ProbeSent bool

	// StalledIntervals counts consecutive heartbeat intervals in which this peer
	// had everything sent to it but has acknowledged none of it.
	//
	// That state is ambiguous: the entries may simply be in flight, or they may
	// have been dropped and will never arrive. Waiting a couple of intervals
	// before assuming loss avoids mistaking ordinary pipelining for failure,
	// while still recovering without waiting for an election timeout.
	StalledIntervals int

	// RecentActive reports whether the peer has responded since the last
	// quorum check. It backs the leader's self-check: a leader that cannot
	// reach a majority must step down, or a partitioned leader would keep
	// serving reads from state the rest of the cluster has moved past.
	RecentActive bool

	// IsLearner mirrors the configuration, so replication decisions do not need
	// to consult it on every message.
	IsLearner bool
}

// newProgress returns progress for a peer whose log is unknown, positioned to
// probe at the leader's next index.
func newProgress(next Index, isLearner bool) *progress {
	return &progress{Next: next, State: stateProbe, IsLearner: isLearner}
}

// becomeProbe moves the peer into probing, resuming just after the last
// acknowledged index.
func (pr *progress) becomeProbe() {
	// Coming out of a snapshot transfer, the follower is known to hold at least
	// PendingSnapshot even if it never acknowledged an entry, so probing may
	// resume from there rather than from Match.
	if pr.State == stateSnapshot {
		pending := pr.PendingSnapshot
		pr.reset(stateProbe)
		pr.Next = max(pr.Match, pending) + 1
		return
	}
	pr.reset(stateProbe)
	pr.Next = pr.Match + 1
}

// becomeReplicate moves the peer into optimistic streaming.
func (pr *progress) becomeReplicate() {
	pr.reset(stateReplicate)
	pr.Next = pr.Match + 1
}

// becomeSnapshot records that a snapshot transfer through index i is in flight.
func (pr *progress) becomeSnapshot(i Index) {
	pr.reset(stateSnapshot)
	pr.PendingSnapshot = i
}

func (pr *progress) reset(s progressState) {
	pr.State = s
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
}

// maybeUpdate advances Match and Next for an acknowledgement of index i,
// reporting whether this was new information.
//
// The guard against regression is not paranoia: responses can be reordered or
// duplicated by the network, and an old acknowledgement that lowered Match
// would let the computed commit index move backwards.
func (pr *progress) maybeUpdate(i Index) bool {
	updated := false
	if pr.Match < i {
		pr.Match = i
		updated = true
		pr.ProbeSent = false
		// Forward progress clears any suspicion that the stream is stuck.
		pr.StalledIntervals = 0
	}
	if pr.Next < i+1 {
		pr.Next = i + 1
	}
	return updated
}

// maybeDecrTo handles a rejection, backing Next up to retry lower in the log.
// It reports whether the rejection was acted on; a stale one is ignored.
//
// hint is where the follower says its log diverges, which lets the leader skip
// an entire conflicting term in one step instead of decrementing by one index
// per round trip.
func (pr *progress) maybeDecrTo(rejected, hint Index) bool {
	if pr.State == stateReplicate {
		// While streaming, Match is authoritative: the follower has already
		// acknowledged up to it. A rejection at or below Match is therefore
		// stale, left over from before the stream was established.
		if rejected <= pr.Match {
			return false
		}
		// Otherwise the optimistic stream was wrong. Fall back to probing from
		// what is actually acknowledged.
		pr.Next = pr.Match + 1
		pr.becomeProbe()
		return true
	}

	// While probing, only a rejection of the exact index in flight is
	// meaningful; anything else is a duplicate or reordered reply.
	if rejected != pr.Next-1 {
		return false
	}

	pr.Next = max(min(rejected, hint+1), 1)
	pr.ProbeSent = false
	return true
}

// isPaused reports whether the leader should withhold entries from this peer.
// Probing is limited to one outstanding message so that the leader interprets
// one rejection at a time, and a snapshot transfer suppresses entries entirely.
func (pr *progress) isPaused() bool {
	switch pr.State {
	case stateProbe:
		return pr.ProbeSent
	case stateSnapshot:
		return true
	default:
		return false
	}
}

func (pr *progress) String() string {
	return fmt.Sprintf("match=%d next=%d state=%s learner=%v active=%v",
		pr.Match, pr.Next, pr.State, pr.IsLearner, pr.RecentActive)
}
