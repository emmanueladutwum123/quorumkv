package server

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
	"github.com/emmanueladutwum123/quorumkv/internal/store"
)

// KVService is the client-facing API.
//
// Writes are only accepted by the leader. A follower refuses them with
// FAILED_PRECONDITION and the leader's address, so a client is redirected in one
// hop instead of discovering the leader by trial and error.
type KVService struct {
	kvv1.UnimplementedKVServiceServer
	srv *Server
}

// NewKVService wraps a node's client API.
func NewKVService(srv *Server) *KVService { return &KVService{srv: srv} }

// Register attaches the service to a gRPC server.
func (k *KVService) Register(s *grpc.Server) { kvv1.RegisterKVServiceServer(s, k) }

// toStatus maps internal errors onto gRPC codes a client can act on.
//
// The distinction that matters is between "ask someone else" and "the outcome is
// unknown". A redirect can be retried immediately against the named leader; a
// lost proposal or a timeout must be retried with the *same* sequence number,
// because the entry may already have committed and only the session table can
// make the repeat safe.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var nl *NotLeaderError
	if errors.As(err, &nl) {
		st := status.New(codes.FailedPrecondition, nl.Error())
		return st.Err()
	}
	switch {
	case errors.Is(err, ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrLeadershipLost):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrTimeout):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, ErrStopped):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// withTimeout applies the node's request timeout when the caller supplied no
// deadline, so a client that forgets one cannot occupy a slot indefinitely.
func (k *KVService) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, k.srv.cfg.RequestTimeout)
}

func (k *KVService) applyCommand(ctx context.Context, cmd *kvv1.Command) (raft.Index, *kvv1.CommandResult, error) {
	data, err := store.EncodeCommand(cmd)
	if err != nil {
		return 0, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := k.withTimeout(ctx)
	defer cancel()

	index, result, err := k.srv.propose(ctx, raft.EntryNormal, data)
	if err != nil {
		return 0, nil, toStatus(err)
	}
	return index, result, nil
}

// Put stores a value, returning the index at which the write committed.
func (k *KVService) Put(ctx context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if len(r.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	index, _, err := k.applyCommand(ctx, &kvv1.Command{
		Op:     kvv1.OpType_OP_TYPE_PUT,
		Key:    r.GetKey(),
		Value:  r.GetValue(),
		Header: r.GetHeader(),
	})
	if err != nil {
		return nil, err
	}
	return &kvv1.PutResponse{CommitIndex: uint64(index)}, nil
}

// Delete removes a key, reporting whether it was there.
func (k *KVService) Delete(ctx context.Context, r *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if len(r.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	index, result, err := k.applyCommand(ctx, &kvv1.Command{
		Op:     kvv1.OpType_OP_TYPE_DELETE,
		Key:    r.GetKey(),
		Header: r.GetHeader(),
	})
	if err != nil {
		return nil, err
	}
	return &kvv1.DeleteResponse{Existed: result.GetExisted(), CommitIndex: uint64(index)}, nil
}

// CompareAndSwap conditionally replaces a value.
func (k *KVService) CompareAndSwap(ctx context.Context, r *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	if len(r.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	index, result, err := k.applyCommand(ctx, &kvv1.Command{
		Op:            kvv1.OpType_OP_TYPE_CAS,
		Key:           r.GetKey(),
		Value:         r.GetNewValue(),
		ExpectedValue: r.GetExpectedValue(),
		ExpectAbsent:  r.GetExpectAbsent(),
		Header:        r.GetHeader(),
	})
	if err != nil {
		return nil, err
	}
	return &kvv1.CompareAndSwapResponse{
		Swapped:      result.GetSwapped(),
		CurrentValue: result.GetCurrentValue(),
		Found:        result.GetFound(),
		CommitIndex:  uint64(index),
	}, nil
}

// Get reads a key.
//
// A linearizable read costs one round trip to a quorum and no log append: the
// leader confirms it still leads, waits for the state machine to catch up to the
// index it recorded, and then reads locally. A stale read skips all of that and
// is served from whatever this replica currently holds.
func (k *KVService) Get(ctx context.Context, r *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if len(r.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if r.GetConsistency() == kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_STALE {
		value, found := k.srv.fsm.Get(r.GetKey())
		return &kvv1.GetResponse{
			Value:     value,
			Found:     found,
			ReadIndex: uint64(k.srv.fsm.AppliedIndex()),
		}, nil
	}

	ctx, cancel := k.withTimeout(ctx)
	defer cancel()

	readIndex, err := k.srv.readIndex(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	// Reaching here means the state machine has applied through readIndex, so a
	// local read now reflects every write acknowledged before this call began.
	value, found := k.srv.fsm.Get(r.GetKey())
	return &kvv1.GetResponse{Value: value, Found: found, ReadIndex: uint64(readIndex)}, nil
}

// Status reports this node's view of the cluster.
func (k *KVService) Status(ctx context.Context, _ *kvv1.StatusRequest) (*kvv1.StatusResponse, error) {
	ctx, cancel := k.withTimeout(ctx)
	defer cancel()

	st, err := k.srv.Status(ctx)
	if err != nil {
		return nil, toStatus(err)
	}

	resp := &kvv1.StatusResponse{
		NodeId:        uint64(st.ID),
		Role:          st.Role.String(),
		Term:          uint64(st.Term),
		LeaderId:      uint64(st.Leader),
		CommitIndex:   uint64(st.Commit),
		AppliedIndex:  uint64(st.Applied),
		LastLogIndex:  uint64(st.LastLog),
		SnapshotIndex: uint64(st.Snapshot),
	}
	for _, id := range st.Config.Members() {
		reachability := k.srv.tr.Health(id).String()
		if id == st.ID {
			reachability = "self"
		}
		peer := &kvv1.PeerStatus{
			NodeId:       uint64(id),
			Address:      k.srv.PeerAddr(id),
			IsLearner:    st.Config.IsLearner(id),
			Reachability: reachability,
		}
		// Match indexes are only known to a leader; no other role tracks them.
		if pr, ok := st.Progress[id]; ok {
			peer.MatchIndex = uint64(pr.Match)
		}
		resp.Peers = append(resp.Peers, peer)
	}
	return resp, nil
}
