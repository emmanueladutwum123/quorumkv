// Package transport carries consensus messages between nodes.
//
// The consensus core is transport-agnostic: it emits and consumes
// raft.Message values and knows nothing about gRPC, sockets, or serialisation.
// This package owns the translation, which is what lets the same core run over a
// real network in production and over an in-memory queue in the simulator.
package transport

import (
	raftv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/raftv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// The conversions below are written out field by field rather than generated or
// reflected over. The wire format is a compatibility contract — during a rolling
// upgrade two builds exchange these messages — so a change to it should require
// an edit here, where it is visible in review, rather than following silently
// from a change to a Go struct.

func entryToProto(e raft.Entry) *raftv1.Entry {
	return &raftv1.Entry{
		Term:  uint64(e.Term),
		Index: uint64(e.Index),
		Type:  entryTypeToProto(e.Type),
		Data:  e.Data,
	}
}

func entryFromProto(e *raftv1.Entry) raft.Entry {
	return raft.Entry{
		Term:  raft.Term(e.GetTerm()),
		Index: raft.Index(e.GetIndex()),
		Type:  entryTypeFromProto(e.GetType()),
		Data:  e.GetData(),
	}
}

// entryTypeToProto converts explicitly rather than casting. The proto enum
// reserves zero for UNSPECIFIED, so the two numbering schemes are offset by one
// and a cast would silently mislabel every entry.
func entryTypeToProto(t raft.EntryType) raftv1.EntryType {
	switch t {
	case raft.EntryNormal:
		return raftv1.EntryType_ENTRY_TYPE_NORMAL
	case raft.EntryNoOp:
		return raftv1.EntryType_ENTRY_TYPE_NO_OP
	case raft.EntryConfChange:
		return raftv1.EntryType_ENTRY_TYPE_CONF_CHANGE
	default:
		return raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED
	}
}

func entryTypeFromProto(t raftv1.EntryType) raft.EntryType {
	switch t {
	case raftv1.EntryType_ENTRY_TYPE_NO_OP:
		return raft.EntryNoOp
	case raftv1.EntryType_ENTRY_TYPE_CONF_CHANGE:
		return raft.EntryConfChange
	default:
		return raft.EntryNormal
	}
}

func entriesToProto(ents []raft.Entry) []*raftv1.Entry {
	if len(ents) == 0 {
		return nil
	}
	out := make([]*raftv1.Entry, len(ents))
	for i := range ents {
		out[i] = entryToProto(ents[i])
	}
	return out
}

func entriesFromProto(ents []*raftv1.Entry) []raft.Entry {
	if len(ents) == 0 {
		return nil
	}
	out := make([]raft.Entry, len(ents))
	for i := range ents {
		out[i] = entryFromProto(ents[i])
	}
	return out
}

func configToProto(c raft.Config) *raftv1.Config {
	toSlice := func(s raft.NodeSet) []uint64 {
		ids := s.Sorted() // sorted so the encoding of a set is byte-stable
		out := make([]uint64, len(ids))
		for i, id := range ids {
			out[i] = uint64(id)
		}
		return out
	}
	return &raftv1.Config{
		IncomingVoters: toSlice(c.Voters[0]),
		OutgoingVoters: toSlice(c.Voters[1]),
		Learners:       toSlice(c.Learners),
	}
}

func configFromProto(c *raftv1.Config) raft.Config {
	toSet := func(ids []uint64) raft.NodeSet {
		s := make(raft.NodeSet, len(ids))
		for _, id := range ids {
			s[raft.NodeID(id)] = struct{}{}
		}
		return s
	}
	var out raft.Config
	out.Voters[0] = toSet(c.GetIncomingVoters())
	out.Voters[1] = toSet(c.GetOutgoingVoters())
	out.Learners = toSet(c.GetLearners())
	return out
}

func snapshotToProto(s *raft.Snapshot) *raftv1.Snapshot {
	if s == nil {
		return nil
	}
	return &raftv1.Snapshot{
		Index: uint64(s.Index),
		Term:  uint64(s.Term),
		Conf:  configToProto(s.Conf),
		Data:  s.Data,
	}
}

func snapshotFromProto(s *raftv1.Snapshot) *raft.Snapshot {
	if s == nil {
		return nil
	}
	return &raft.Snapshot{
		Index: raft.Index(s.GetIndex()),
		Term:  raft.Term(s.GetTerm()),
		Conf:  configFromProto(s.GetConf()),
		Data:  s.GetData(),
	}
}

// --- request encoding ------------------------------------------------------

func voteRequestToProto(m raft.Message) *raftv1.VoteRequest {
	return &raftv1.VoteRequest{
		Term:         uint64(m.Term),
		CandidateId:  uint64(m.From),
		LastLogIndex: uint64(m.LastLogIndex),
		LastLogTerm:  uint64(m.LastLogTerm),
		PreVote:      m.PreVote,
	}
}

func voteRequestFromProto(r *raftv1.VoteRequest, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:         raft.MsgVoteReq,
		From:         raft.NodeID(r.GetCandidateId()),
		To:           to,
		Term:         raft.Term(r.GetTerm()),
		LastLogIndex: raft.Index(r.GetLastLogIndex()),
		LastLogTerm:  raft.Term(r.GetLastLogTerm()),
		PreVote:      r.GetPreVote(),
	}
}

func voteResponseToProto(m raft.Message) *raftv1.VoteResponse {
	return &raftv1.VoteResponse{
		Term:    uint64(m.Term),
		VoterId: uint64(m.From),
		Granted: m.Granted,
		PreVote: m.PreVote,
	}
}

func voteResponseFromProto(r *raftv1.VoteResponse, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:    raft.MsgVoteResp,
		From:    raft.NodeID(r.GetVoterId()),
		To:      to,
		Term:    raft.Term(r.GetTerm()),
		Granted: r.GetGranted(),
		PreVote: r.GetPreVote(),
	}
}

func appendRequestToProto(m raft.Message) *raftv1.AppendRequest {
	return &raftv1.AppendRequest{
		Term:         uint64(m.Term),
		LeaderId:     uint64(m.From),
		PrevLogIndex: uint64(m.PrevLogIndex),
		PrevLogTerm:  uint64(m.PrevLogTerm),
		Entries:      entriesToProto(m.Entries),
		LeaderCommit: uint64(m.Commit),
		ReadCtx:      m.ReadCtx,
	}
}

func appendRequestFromProto(r *raftv1.AppendRequest, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:         raft.MsgAppReq,
		From:         raft.NodeID(r.GetLeaderId()),
		To:           to,
		Term:         raft.Term(r.GetTerm()),
		PrevLogIndex: raft.Index(r.GetPrevLogIndex()),
		PrevLogTerm:  raft.Term(r.GetPrevLogTerm()),
		Entries:      entriesFromProto(r.GetEntries()),
		Commit:       raft.Index(r.GetLeaderCommit()),
		ReadCtx:      r.GetReadCtx(),
	}
}

func appendResponseToProto(m raft.Message) *raftv1.AppendResponse {
	return &raftv1.AppendResponse{
		Term:       uint64(m.Term),
		FollowerId: uint64(m.From),
		Reject:     m.Reject,
		MatchIndex: uint64(m.MatchIndex),
		HintIndex:  uint64(m.HintIndex),
		HintTerm:   uint64(m.HintTerm),
		ReadCtx:    m.ReadCtx,
	}
}

func appendResponseFromProto(r *raftv1.AppendResponse, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:       raft.MsgAppResp,
		From:       raft.NodeID(r.GetFollowerId()),
		To:         to,
		Term:       raft.Term(r.GetTerm()),
		Reject:     r.GetReject(),
		MatchIndex: raft.Index(r.GetMatchIndex()),
		HintIndex:  raft.Index(r.GetHintIndex()),
		HintTerm:   raft.Term(r.GetHintTerm()),
		ReadCtx:    r.GetReadCtx(),
	}
}

func snapshotRequestToProto(m raft.Message) *raftv1.SnapshotRequest {
	return &raftv1.SnapshotRequest{
		Term:     uint64(m.Term),
		LeaderId: uint64(m.From),
		Snapshot: snapshotToProto(m.Snapshot),
	}
}

func snapshotRequestFromProto(r *raftv1.SnapshotRequest, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:     raft.MsgSnapReq,
		From:     raft.NodeID(r.GetLeaderId()),
		To:       to,
		Term:     raft.Term(r.GetTerm()),
		Snapshot: snapshotFromProto(r.GetSnapshot()),
	}
}

func snapshotResponseToProto(m raft.Message) *raftv1.SnapshotResponse {
	return &raftv1.SnapshotResponse{
		Term:       uint64(m.Term),
		FollowerId: uint64(m.From),
		Reject:     m.Reject,
		MatchIndex: uint64(m.MatchIndex),
	}
}

func snapshotResponseFromProto(r *raftv1.SnapshotResponse, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:       raft.MsgSnapResp,
		From:       raft.NodeID(r.GetFollowerId()),
		To:         to,
		Term:       raft.Term(r.GetTerm()),
		Reject:     r.GetReject(),
		MatchIndex: raft.Index(r.GetMatchIndex()),
	}
}

func readIndexRequestToProto(m raft.Message) *raftv1.ReadIndexRequest {
	return &raftv1.ReadIndexRequest{
		Term:    uint64(m.Term),
		FromId:  uint64(m.From),
		ReadCtx: m.ReadCtx,
	}
}

func readIndexRequestFromProto(r *raftv1.ReadIndexRequest, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:    raft.MsgReadIndexReq,
		From:    raft.NodeID(r.GetFromId()),
		To:      to,
		Term:    raft.Term(r.GetTerm()),
		ReadCtx: r.GetReadCtx(),
	}
}

func readIndexResponseToProto(m raft.Message) *raftv1.ReadIndexResponse {
	return &raftv1.ReadIndexResponse{
		Term:      uint64(m.Term),
		FromId:    uint64(m.From),
		Reject:    m.Reject,
		ReadIndex: uint64(m.ReadIndex),
		ReadCtx:   m.ReadCtx,
	}
}

func readIndexResponseFromProto(r *raftv1.ReadIndexResponse, to raft.NodeID) raft.Message {
	return raft.Message{
		Type:      raft.MsgReadIndexResp,
		From:      raft.NodeID(r.GetFromId()),
		To:        to,
		Term:      raft.Term(r.GetTerm()),
		Reject:    r.GetReject(),
		ReadIndex: raft.Index(r.GetReadIndex()),
		ReadCtx:   r.GetReadCtx(),
	}
}
