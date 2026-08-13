// Package raft implements the Raft consensus algorithm as a deterministic,
// side-effect-free state machine.
//
// The core deliberately performs no I/O, starts no goroutines, reads no clock
// and touches no network. All input arrives through Step and Tick; all output
// leaves through Ready. That constraint is what makes the cluster testable: a
// seeded scheduler can drive thousands of nodes through partitions, crashes and
// clock skew reproducibly, because nothing here can observe the outside world.
//
// Layering:
//
//	transport (gRPC)  ->  Step(Message)  ->  raft core  ->  Ready()  ->  WAL + FSM
//
// The caller owns durability. Ready describes what must be persisted, in what
// order, before the accompanying messages may be sent.
package raft

import (
	"errors"
	"fmt"
)

// NodeID identifies a cluster member. Zero is reserved to mean "no node",
// which lets an unset Vote be distinguished from a vote for a real member.
type NodeID uint64

// None is the zero NodeID: no leader known, or no vote cast this term.
const None NodeID = 0

// Term is a Raft logical clock value. Terms increase monotonically and every
// message carries one; a node seeing a higher term steps down immediately.
type Term uint64

// Index is a position in the replicated log. The first real entry is at index
// 1, so index 0 always denotes "before the beginning of the log".
type Index uint64

// Role is the state a node occupies within a term.
type Role uint8

const (
	// Follower accepts entries from a leader and votes at most once per term.
	Follower Role = iota
	// Candidate is soliciting votes for its own election.
	Candidate
	// Leader is the single node in a term permitted to append client entries.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// EntryType distinguishes payloads that the state machine applies from those
// the consensus layer interprets itself.
type EntryType uint8

const (
	// EntryNormal carries an opaque client command for the FSM.
	EntryNormal EntryType = iota
	// EntryNoOp is the blank entry a new leader commits to establish its
	// commit index safely. See the note on Raft §5.4.2 in raft.go.
	EntryNoOp
	// EntryConfChange carries a cluster membership change, interpreted by the
	// consensus layer as it is applied.
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoOp:
		return "noop"
	case EntryConfChange:
		return "confchange"
	default:
		return fmt.Sprintf("entrytype(%d)", uint8(t))
	}
}

// Entry is one record in the replicated log. The (Term, Index) pair is the
// identity Raft reasons about: if two logs hold an entry with the same term and
// index, the algorithm guarantees the payloads are identical and that every
// preceding entry matches too.
type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

// HardState is the subset of node state that must survive a crash. Losing any
// of it can break Raft's safety guarantees: a forgotten vote allows two leaders
// in one term, and a forgotten term allows a stale log to win an election.
type HardState struct {
	// Term is the node's current term.
	Term Term
	// Vote is the candidate this node voted for in Term, or None.
	Vote NodeID
	// Commit is the highest index known to be committed. Persisting it is an
	// optimisation rather than a requirement, but it lets a restarted node
	// re-apply to its FSM without waiting for the leader.
	Commit Index
}

// IsEmpty reports whether hs carries no information worth persisting.
func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.Vote == None && hs.Commit == 0
}

// Snapshot is a compacted prefix of the log: the FSM state as of Index, plus
// the membership in force at that point. Index and Term must match the log
// entry the snapshot replaces so that the AppendEntries consistency check keeps
// working across the compaction boundary.
type Snapshot struct {
	Index Index
	Term  Term
	// Conf is the cluster membership as of Index. It travels with the snapshot
	// because a restoring node has no log left to replay conf changes from.
	Conf Config
	Data []byte
}

// MessageType enumerates the RPCs of the protocol. The paper's three RPCs are
// modelled as request/response pairs; ReadIndex and TimeoutNow are the two
// standard extensions used for linearizable reads and leadership transfer.
type MessageType uint8

const (
	// MsgVoteReq is RequestVote (§5.2).
	MsgVoteReq MessageType = iota
	// MsgVoteResp answers RequestVote.
	MsgVoteResp
	// MsgAppReq is AppendEntries (§5.3), also serving as the heartbeat when
	// Entries is empty.
	MsgAppReq
	// MsgAppResp answers AppendEntries.
	MsgAppResp
	// MsgSnapReq is InstallSnapshot (§7), sent when a follower has fallen
	// behind the leader's compaction boundary.
	MsgSnapReq
	// MsgSnapResp answers InstallSnapshot.
	MsgSnapResp
	// MsgReadIndexReq asks the leader to confirm its leadership so a read can
	// be served linearizably without appending to the log (§6.4).
	MsgReadIndexReq
	// MsgReadIndexResp carries the confirmed read index.
	MsgReadIndexResp
	// MsgTimeoutNow instructs a follower to start an election immediately,
	// used for graceful leadership transfer.
	MsgTimeoutNow
)

func (t MessageType) String() string {
	switch t {
	case MsgVoteReq:
		return "VoteReq"
	case MsgVoteResp:
		return "VoteResp"
	case MsgAppReq:
		return "AppReq"
	case MsgAppResp:
		return "AppResp"
	case MsgSnapReq:
		return "SnapReq"
	case MsgSnapResp:
		return "SnapResp"
	case MsgReadIndexReq:
		return "ReadIndexReq"
	case MsgReadIndexResp:
		return "ReadIndexResp"
	case MsgTimeoutNow:
		return "TimeoutNow"
	default:
		return fmt.Sprintf("msg(%d)", uint8(t))
	}
}

// Message is the single wire type of the protocol. One flat struct covers every
// RPC because it keeps Step's signature stable and makes the transport codec
// trivial; each field's owning message types are noted below.
//
// The core never mutates a Message it receives, and never retains one after
// Step returns, so callers may reuse buffers between calls.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	// Term is the sender's term. The core's first act in Step is to compare
	// this against its own, which is where step-down and stale-message
	// rejection are centralised.
	Term Term

	// LastLogIndex and LastLogTerm describe a candidate's log (MsgVoteReq),
	// and are compared against the voter's own log by the §5.4.1 election
	// restriction.
	LastLogIndex Index
	LastLogTerm  Term

	// PrevLogIndex and PrevLogTerm form the AppendEntries consistency check
	// (MsgAppReq): the follower accepts Entries only if it holds a matching
	// entry at this position.
	PrevLogIndex Index
	PrevLogTerm  Term
	// Entries are the log entries to append (MsgAppReq). Empty for heartbeats.
	Entries []Entry
	// Commit is the leader's commit index (MsgAppReq), letting followers
	// advance their own without a separate round trip.
	Commit Index

	// Granted reports whether a vote was given (MsgVoteResp).
	Granted bool
	// Reject reports whether the consistency check failed (MsgAppResp), or the
	// read was refused (MsgReadIndexResp).
	Reject bool
	// MatchIndex is the highest index the follower has accepted (MsgAppResp on
	// success), which drives the leader's commit computation.
	MatchIndex Index
	// HintIndex and HintTerm let a rejecting follower describe where its log
	// actually diverges (MsgAppResp on rejection), so the leader can skip a
	// whole conflicting term instead of decrementing one index per round trip.
	HintIndex Index
	HintTerm  Term

	// Snapshot carries the compacted state (MsgSnapReq).
	Snapshot *Snapshot

	// ReadCtx is an opaque token echoed back to the caller so a ReadIndex
	// response can be matched to its request (MsgReadIndex*).
	ReadCtx []byte
	// ReadIndex is the index a read must observe to be linearizable
	// (MsgReadIndexResp).
	ReadIndex Index
}

// ReadState is the resolved answer to a linearizable read request: once the FSM
// has applied through Index, a local read is safe to serve for ReadCtx.
type ReadState struct {
	Index   Index
	ReadCtx []byte
}

// SoftState is derived, non-durable state. It is reported for observability and
// client routing, and must never be persisted: recomputing it after a restart
// is always correct, whereas a stale persisted copy is not.
type SoftState struct {
	Leader NodeID
	Role   Role
}

// Ready is the batch of work the core hands back to its driver. The ordering
// contract is not advisory — violating it breaks durability:
//
//  1. Persist HardState and Entries (and Snapshot, if present) to stable
//     storage, fsyncing before continuing.
//  2. Send Messages to their destinations.
//  3. Apply Committed to the state machine, then deliver ReadStates.
//  4. Call Advance to acknowledge the batch.
//
// Step 1 must precede step 2 because a node that promises a vote or an entry it
// then forgets across a crash can produce two leaders in one term, or lose a
// committed write.
type Ready struct {
	// SoftState is set only when the leader or role changed.
	SoftState *SoftState
	// HardState is set only when durable state changed.
	HardState *HardState
	// Snapshot, when set, must be persisted and applied before Entries.
	Snapshot *Snapshot
	// Entries must be appended to stable storage.
	Entries []Entry
	// Committed entries are safe to apply to the state machine.
	Committed []Entry
	// Messages must be sent after Entries and HardState are durable.
	Messages []Message
	// ReadStates are linearizable reads that are now safe to serve.
	ReadStates []ReadState
}

// IsEmpty reports whether there is no work in the batch, letting the driver
// skip a wakeup entirely.
func (rd Ready) IsEmpty() bool {
	return rd.SoftState == nil && rd.HardState == nil && rd.Snapshot == nil &&
		len(rd.Entries) == 0 && len(rd.Committed) == 0 &&
		len(rd.Messages) == 0 && len(rd.ReadStates) == 0
}

// Errors returned by log and storage access. They are sentinel values because
// callers must distinguish "this index was compacted away" (recoverable: send a
// snapshot) from "this index does not exist yet" (a protocol bug).
var (
	// ErrCompacted means the requested index precedes the snapshot boundary.
	ErrCompacted = errors.New("raft: requested index is below the compaction boundary")
	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("raft: requested index is not yet in the log")
	// ErrSnapshotOutOfDate means a snapshot older than the current one was
	// offered, and applying it would move the state machine backwards.
	ErrSnapshotOutOfDate = errors.New("raft: snapshot is older than current state")
	// ErrProposalDropped means a proposal could not be accepted, typically
	// because this node is not the leader.
	ErrProposalDropped = errors.New("raft: proposal dropped")
	// ErrStopped means the node has been shut down.
	ErrStopped = errors.New("raft: node is stopped")
)
