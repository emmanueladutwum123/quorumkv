package raft

import (
	"encoding/binary"
	"fmt"
)

// ConfChangeType is a membership operation.
type ConfChangeType uint8

const (
	// ConfChangeAddVoter adds a voting member.
	//
	// Prefer adding as a learner and promoting once caught up. A new voter that is
	// still transferring a snapshot counts toward quorum while being unable to
	// satisfy it: in a three-node cluster, adding a fourth voter raises the
	// requirement to three while leaving only three nodes able to meet it, so a
	// single further failure stalls commits entirely.
	ConfChangeAddVoter ConfChangeType = iota
	// ConfChangeAddLearner adds a non-voting member that replicates the log.
	ConfChangeAddLearner
	// ConfChangeRemoveNode removes a voter or learner.
	ConfChangeRemoveNode
	// ConfChangePromoteLearner turns a caught-up learner into a voter.
	ConfChangePromoteLearner
	// ConfChangeLeaveJoint completes a transition by retiring the outgoing voter
	// set. The leader proposes it automatically; it is not for callers.
	ConfChangeLeaveJoint
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfChangeAddVoter:
		return "add-voter"
	case ConfChangeAddLearner:
		return "add-learner"
	case ConfChangeRemoveNode:
		return "remove-node"
	case ConfChangePromoteLearner:
		return "promote-learner"
	case ConfChangeLeaveJoint:
		return "leave-joint"
	default:
		return fmt.Sprintf("confchange(%d)", uint8(t))
	}
}

// ConfChange is a membership change carried in an EntryConfChange log entry.
type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
	// Context travels with the change so that a node learning of a new member
	// from the log also learns how to reach it. Without this, a restarted node
	// would recover a membership containing ids it has no address for.
	Context []byte
}

// Encode serialises a change for the log.
//
// The encoding lives in this package, and is hand-rolled, because the consensus
// core must interpret these entries itself — it cannot depend on the driver's
// serialisation choices, and it must be able to replay entries written by an
// earlier build.
func (cc ConfChange) Encode() []byte {
	buf := make([]byte, 1+8+4+len(cc.Context))
	buf[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(buf[1:9], uint64(cc.NodeID))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(cc.Context)))
	copy(buf[13:], cc.Context)
	return buf
}

// DecodeConfChange parses a change from a log entry's payload.
func DecodeConfChange(data []byte) (ConfChange, error) {
	if len(data) < 13 {
		return ConfChange{}, fmt.Errorf("raft: conf change payload is %d bytes, want at least 13", len(data))
	}
	ctxLen := binary.LittleEndian.Uint32(data[9:13])
	if int(ctxLen) != len(data)-13 {
		return ConfChange{}, fmt.Errorf("raft: conf change context length %d does not match payload", ctxLen)
	}
	cc := ConfChange{
		Type:   ConfChangeType(data[0]),
		NodeID: NodeID(binary.LittleEndian.Uint64(data[1:9])),
	}
	if ctxLen > 0 {
		cc.Context = make([]byte, ctxLen)
		copy(cc.Context, data[13:])
	}
	return cc, nil
}

// withChange returns the configuration that results from applying cc.
//
// Changes to the *voter* sets enter a joint configuration, in which every quorum
// decision must be satisfied in both the incoming and outgoing sets. Changes that
// only touch learners do not, because a learner never counts toward quorum and so
// cannot create two disjoint majorities.
func (c Config) withChange(cc ConfChange) (Config, error) {
	next := c.Clone()

	switch cc.Type {
	case ConfChangeLeaveJoint:
		if !c.IsJoint() {
			return c, fmt.Errorf("raft: leave-joint with no transition in flight")
		}
		next.Voters[1] = NewNodeSet()
		return next, nil

	case ConfChangeAddLearner:
		if cc.NodeID == None {
			return c, fmt.Errorf("raft: node id 0 is reserved")
		}
		if c.IsVoter(cc.NodeID) {
			return c, fmt.Errorf("raft: node %d is already a voter; remove it before adding it as a learner", cc.NodeID)
		}
		if c.IsLearner(cc.NodeID) {
			// Already in the desired state. Returning unchanged keeps a repeated
			// request from entering a pointless transition.
			return c, nil
		}
		next.Learners[cc.NodeID] = struct{}{}
		return next, nil

	case ConfChangeRemoveNode:
		if c.IsLearner(cc.NodeID) && !c.IsVoter(cc.NodeID) {
			// Removing a learner changes no quorum, so it needs no joint step.
			delete(next.Learners, cc.NodeID)
			return next, nil
		}
		if !c.IsVoter(cc.NodeID) {
			return c, nil // not a member; nothing to do
		}
	}

	// Everything below alters the voter set.
	if c.IsJoint() {
		// Overlapping transitions would make the quorum requirement ambiguous:
		// there is only one slot for an outgoing set, so a second change would
		// discard the first's safety guarantee.
		return c, fmt.Errorf("raft: a membership change is already in flight")
	}

	switch cc.Type {
	case ConfChangeAddVoter:
		if cc.NodeID == None {
			return c, fmt.Errorf("raft: node id 0 is reserved")
		}
		if c.Voters[0].Contains(cc.NodeID) {
			return c, nil // already a voter
		}
		next.Voters[0][cc.NodeID] = struct{}{}
		delete(next.Learners, cc.NodeID)

	case ConfChangePromoteLearner:
		if !c.IsLearner(cc.NodeID) {
			return c, fmt.Errorf("raft: node %d is not a learner", cc.NodeID)
		}
		next.Voters[0][cc.NodeID] = struct{}{}
		delete(next.Learners, cc.NodeID)

	case ConfChangeRemoveNode:
		delete(next.Voters[0], cc.NodeID)

	default:
		return c, fmt.Errorf("raft: unknown conf change type %d", uint8(cc.Type))
	}

	if len(next.Voters[0]) == 0 {
		// Removing the last voter would leave a cluster that can never elect a
		// leader again, and no later change could fix it.
		return c, fmt.Errorf("raft: change would leave the cluster with no voters")
	}

	// Record the configuration being replaced, which is what makes the transition
	// safe: until the change commits, both majorities are required.
	next.Voters[1] = c.Voters[0].Clone()

	if err := next.Validate(); err != nil {
		return c, err
	}
	return next, nil
}

// ProposeConfChange appends a membership change to the log.
//
// The change takes effect when the entry is *applied*, not when it is proposed.
// Only one may be in flight at a time: a second change proposed before the first
// commits could be built on a configuration that never becomes real.
func (n *Node) ProposeConfChange(cc ConfChange) (Index, error) {
	if n.role != Leader {
		return 0, ErrProposalDropped
	}
	if n.cfg.IsJoint() {
		return 0, fmt.Errorf("raft: a membership change is already in flight")
	}
	if n.pendingConfIndex > n.log.applied {
		// A change is in the log but not yet applied, so the configuration it
		// produces is not known. Validating this one against the current
		// configuration could therefore accept something unsafe.
		return 0, fmt.Errorf("raft: a membership change is proposed but not yet applied")
	}
	// Reject changes that could not be applied, so a client learns immediately
	// rather than watching an entry commit and silently do nothing.
	if _, err := n.cfg.withChange(cc); err != nil {
		return 0, err
	}

	index, err := n.appendEntry(EntryConfChange, cc.Encode())
	if err != nil {
		return 0, err
	}
	n.pendingConfIndex = index
	return index, nil
}

// ApplyConfChange installs a membership change. The driver calls it when it
// applies an EntryConfChange, and must call it for every such entry — the
// configuration is part of replicated state, so skipping one on any replica would
// leave that node computing quorum against the wrong set.
func (n *Node) ApplyConfChange(cc ConfChange) (Config, error) {
	// Completing a transition that is already complete is treated as a no-op
	// rather than an error. A node that restored a snapshot adopted the
	// configuration that snapshot carried, which may already be past the
	// transition, and it will still replay the leave-joint entry from the log.
	// Failing there would stop a replica that is doing nothing wrong.
	if cc.Type == ConfChangeLeaveJoint && !n.cfg.IsJoint() {
		return n.cfg.Clone(), nil
	}

	next, err := n.cfg.withChange(cc)
	if err != nil {
		return n.cfg.Clone(), err
	}
	wasVoter := n.cfg.IsVoter(n.id)
	n.cfg = next
	n.resetProgress()

	if n.role != Leader {
		return n.cfg.Clone(), nil
	}

	// A leader that is no longer a voter cannot be part of any quorum, so it can
	// neither commit nor prove leadership. Stepping down lets the remaining
	// members elect someone who can.
	if wasVoter && !n.cfg.IsVoter(n.id) {
		n.becomeFollower(n.term, None)
		return n.cfg.Clone(), nil
	}

	if n.cfg.IsJoint() {
		// The transition is half-done: finish it automatically once this entry is
		// applied, so the cluster does not sit in the joint state — where it needs
		// two majorities and is therefore less available — any longer than needed.
		if _, err := n.autoLeaveJoint(); err != nil {
			return n.cfg.Clone(), err
		}
		return n.cfg.Clone(), nil
	}

	// The quorum requirement may have shrunk, which can make an entry already in
	// the log committed without any new acknowledgement arriving. Broadcast either
	// way: a newly added member needs to hear from the leader promptly rather than
	// waiting out an election timeout and campaigning.
	n.maybeCommit()
	n.bcastAppend()
	return n.cfg.Clone(), nil
}

// autoLeaveJoint proposes the entry that retires the outgoing voter set.
func (n *Node) autoLeaveJoint() (Index, error) {
	cc := ConfChange{Type: ConfChangeLeaveJoint}
	index, err := n.appendEntry(EntryConfChange, cc.Encode())
	if err != nil {
		return 0, err
	}
	n.pendingConfIndex = index
	return index, nil
}

// PendingConfChange reports whether a membership change is proposed or in flight.
func (n *Node) PendingConfChange() bool {
	return n.cfg.IsJoint() || n.pendingConfIndex > n.log.applied
}
