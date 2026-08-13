package transport

import (
	"testing"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// A codec bug corrupts the protocol silently: messages still flow, but carry
// wrong terms or indexes, and the resulting misbehaviour looks like a consensus
// bug rather than a serialisation one. These tests pin every field.

func TestVoteRequestRoundTrip(t *testing.T) {
	want := raft.Message{
		Type: raft.MsgVoteReq, From: 3, To: 7, Term: 12,
		LastLogIndex: 400, LastLogTerm: 11, PreVote: true,
	}
	got := voteRequestFromProto(voteRequestToProto(want), 7)

	if got.Type != want.Type || got.From != want.From || got.To != want.To ||
		got.Term != want.Term || got.LastLogIndex != want.LastLogIndex ||
		got.LastLogTerm != want.LastLogTerm || got.PreVote != want.PreVote {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

func TestVoteResponseRoundTripPreservesPreVote(t *testing.T) {
	// A granted pre-vote that lost its flag would be counted toward a real
	// election, which is exactly the confusion the flag exists to prevent.
	for _, preVote := range []bool{false, true} {
		want := raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 5, Granted: true, PreVote: preVote}
		got := voteResponseFromProto(voteResponseToProto(want), 1)
		if got.PreVote != preVote || !got.Granted || got.Term != 5 {
			t.Errorf("pre-vote %v: round trip = %+v", preVote, got)
		}
	}
}

func TestAppendRequestRoundTrip(t *testing.T) {
	want := raft.Message{
		Type: raft.MsgAppReq, From: 1, To: 2, Term: 9,
		PrevLogIndex: 100, PrevLogTerm: 8,
		Entries: []raft.Entry{
			{Term: 9, Index: 101, Type: raft.EntryNormal, Data: []byte("payload")},
			{Term: 9, Index: 102, Type: raft.EntryNoOp},
			{Term: 9, Index: 103, Type: raft.EntryConfChange, Data: []byte("membership")},
		},
		Commit:  99,
		ReadCtx: []byte("read-token"),
	}
	got := appendRequestFromProto(appendRequestToProto(want), 2)

	if got.PrevLogIndex != want.PrevLogIndex || got.PrevLogTerm != want.PrevLogTerm ||
		got.Commit != want.Commit || string(got.ReadCtx) != string(want.ReadCtx) {
		t.Errorf("header round trip = %+v, want %+v", got, want)
	}
	if len(got.Entries) != len(want.Entries) {
		t.Fatalf("got %d entries, want %d", len(got.Entries), len(want.Entries))
	}
	for i := range want.Entries {
		w, g := want.Entries[i], got.Entries[i]
		if g.Term != w.Term || g.Index != w.Index || g.Type != w.Type || string(g.Data) != string(w.Data) {
			t.Errorf("entry %d = %+v, want %+v", i, g, w)
		}
	}
}

func TestEntryTypesSurviveTheEnumOffset(t *testing.T) {
	// The proto enum reserves zero for UNSPECIFIED, so the Go and wire numbering
	// differ by one. A cast instead of a conversion would mislabel every entry —
	// silently turning a normal command into a no-op, or a config change into a
	// command the state machine would try to decode.
	for _, want := range []raft.EntryType{raft.EntryNormal, raft.EntryNoOp, raft.EntryConfChange} {
		got := entryTypeFromProto(entryTypeToProto(want))
		if got != want {
			t.Errorf("entry type %v round-tripped to %v", want, got)
		}
	}
	// A no-op must not be indistinguishable from a normal entry on the wire.
	if entryTypeToProto(raft.EntryNormal) == entryTypeToProto(raft.EntryNoOp) {
		t.Error("normal and no-op entries encode identically")
	}
}

func TestAppendResponseRoundTrip(t *testing.T) {
	want := raft.Message{
		Type: raft.MsgAppResp, From: 2, To: 1, Term: 9,
		Reject: true, MatchIndex: 50, HintIndex: 20, HintTerm: 4,
		ReadCtx: []byte("token"),
	}
	got := appendResponseFromProto(appendResponseToProto(want), 1)

	if got.Reject != want.Reject || got.MatchIndex != want.MatchIndex ||
		got.HintIndex != want.HintIndex || got.HintTerm != want.HintTerm ||
		string(got.ReadCtx) != string(want.ReadCtx) {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

func TestSnapshotRoundTripPreservesMembership(t *testing.T) {
	// A restoring node has no log left to replay membership from, so losing the
	// configuration here would leave it unable to identify its own peers.
	want := raft.Message{
		Type: raft.MsgSnapReq, From: 1, To: 4, Term: 6,
		Snapshot: &raft.Snapshot{
			Index: 500, Term: 5,
			Conf: raft.Config{
				Voters:   [2]raft.NodeSet{raft.NewNodeSet(1, 2, 3), raft.NewNodeSet(4, 5)},
				Learners: raft.NewNodeSet(9),
			},
			Data: []byte("state machine contents"),
		},
	}
	got := snapshotRequestFromProto(snapshotRequestToProto(want), 4)

	if got.Snapshot == nil {
		t.Fatal("the snapshot was lost")
	}
	if got.Snapshot.Index != 500 || got.Snapshot.Term != 5 {
		t.Errorf("boundary = (%d, %d), want (500, 5)", got.Snapshot.Index, got.Snapshot.Term)
	}
	if string(got.Snapshot.Data) != "state machine contents" {
		t.Errorf("data = %q", got.Snapshot.Data)
	}
	cfg := got.Snapshot.Conf
	if !cfg.IsJoint() {
		t.Error("the joint configuration was flattened")
	}
	for _, id := range []raft.NodeID{1, 2, 3} {
		if !cfg.Voters[0].Contains(id) {
			t.Errorf("incoming voter %d lost", id)
		}
	}
	for _, id := range []raft.NodeID{4, 5} {
		if !cfg.Voters[1].Contains(id) {
			t.Errorf("outgoing voter %d lost", id)
		}
	}
	if !cfg.IsLearner(9) {
		t.Error("learner 9 lost")
	}
}

func TestNilSnapshotStaysNil(t *testing.T) {
	if got := snapshotToProto(nil); got != nil {
		t.Error("a nil snapshot encoded to a non-nil message")
	}
	if got := snapshotFromProto(nil); got != nil {
		t.Error("a nil snapshot message decoded to a non-nil snapshot")
	}
}

func TestReadIndexRoundTrip(t *testing.T) {
	req := raft.Message{Type: raft.MsgReadIndexReq, From: 2, To: 1, Term: 4, ReadCtx: []byte("abc")}
	gotReq := readIndexRequestFromProto(readIndexRequestToProto(req), 1)
	if gotReq.From != 2 || gotReq.Term != 4 || string(gotReq.ReadCtx) != "abc" {
		t.Errorf("request round trip = %+v", gotReq)
	}

	resp := raft.Message{Type: raft.MsgReadIndexResp, From: 1, To: 2, Term: 4, ReadIndex: 77, ReadCtx: []byte("abc")}
	gotResp := readIndexResponseFromProto(readIndexResponseToProto(resp), 2)
	if gotResp.ReadIndex != 77 || string(gotResp.ReadCtx) != "abc" || gotResp.Reject {
		t.Errorf("response round trip = %+v", gotResp)
	}

	rejected := raft.Message{Type: raft.MsgReadIndexResp, From: 1, To: 2, Term: 4, Reject: true, ReadCtx: []byte("x")}
	gotRejected := readIndexResponseFromProto(readIndexResponseToProto(rejected), 2)
	if !gotRejected.Reject {
		t.Error("a rejection round-tripped as an approval, which would serve a stale read")
	}
}

func TestConfigEncodingIsStable(t *testing.T) {
	// Node sets are unordered in memory but must encode identically every time, or
	// two nodes holding the same membership would disagree byte-for-byte.
	cfg := raft.Config{
		Voters:   [2]raft.NodeSet{raft.NewNodeSet(9, 3, 7, 1), raft.NewNodeSet()},
		Learners: raft.NewNodeSet(20, 10),
	}
	first := configToProto(cfg)
	for i := 0; i < 20; i++ {
		next := configToProto(cfg)
		if len(next.IncomingVoters) != len(first.IncomingVoters) {
			t.Fatal("voter count changed between encodings")
		}
		for j := range first.IncomingVoters {
			if next.IncomingVoters[j] != first.IncomingVoters[j] {
				t.Fatalf("encoding %d differs at voter %d", i, j)
			}
		}
		for j := range first.Learners {
			if next.Learners[j] != first.Learners[j] {
				t.Fatalf("encoding %d differs at learner %d", i, j)
			}
		}
	}
	// And it must be sorted, which is what makes it stable.
	for i := 1; i < len(first.IncomingVoters); i++ {
		if first.IncomingVoters[i-1] >= first.IncomingVoters[i] {
			t.Errorf("voters are not sorted ascending: %v", first.IncomingVoters)
			break
		}
	}
}

func TestEmptyEntriesEncodeAsNil(t *testing.T) {
	// Heartbeats are empty appends and are by far the most common message, so they
	// should not carry an allocated empty slice.
	if got := entriesToProto(nil); got != nil {
		t.Error("nil entries encoded to a non-nil slice")
	}
	if got := entriesFromProto(nil); got != nil {
		t.Error("nil entries decoded to a non-nil slice")
	}
}

func TestResponseTypesAreNotSentByTransport(t *testing.T) {
	// Responses exist only as RPC return values. If the core ever asked the
	// transport to send one, it would be silently dropped, so this pins the set
	// the driver must intercept.
	for _, typ := range []raft.MessageType{
		raft.MsgVoteResp, raft.MsgAppResp, raft.MsgSnapResp, raft.MsgReadIndexResp,
	} {
		if !isResponseType(typ) {
			t.Errorf("%s is not recognised as a response type", typ)
		}
	}
	for _, typ := range []raft.MessageType{
		raft.MsgVoteReq, raft.MsgAppReq, raft.MsgSnapReq, raft.MsgReadIndexReq, raft.MsgTimeoutNow,
	} {
		if isResponseType(typ) {
			t.Errorf("%s was misclassified as a response type", typ)
		}
	}
}

// isResponseType mirrors the driver's classification, kept here so the codec's
// tests can assert on it directly.
func isResponseType(t raft.MessageType) bool {
	switch t {
	case raft.MsgVoteResp, raft.MsgAppResp, raft.MsgSnapResp, raft.MsgReadIndexResp:
		return true
	default:
		return false
	}
}
