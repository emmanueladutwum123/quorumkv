package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/raftv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// Handler receives messages arriving from peers. Implementations must not block:
// the driver's single-threaded loop is what consumes them, and blocking here
// would apply backpressure from one slow peer to the whole node.
type Handler func(raft.Message)

// Transport delivers consensus messages to peers.
//
// Send is fire-and-forget and never returns an error. That is not laziness: Raft
// already treats the network as unreliable, and every message it sends is either
// retried by a heartbeat or made redundant by a later one. Surfacing per-message
// failures would invite the driver to build a second, weaker retry mechanism on
// top of the one the protocol already has.
type Transport interface {
	Send(m raft.Message)
	AddPeer(id raft.NodeID, addr string)
	RemovePeer(id raft.NodeID)
	Peers() map[raft.NodeID]string
	Health(id raft.NodeID) PeerHealth
	Close() error
}

// PeerHealth is the last known state of a link to a peer.
type PeerHealth uint8

const (
	// PeerUnknown means no exchange has been attempted yet. This is the normal
	// state on a follower, which never initiates peer traffic, and is why health
	// cannot be modelled as a boolean: reporting "down" here would show every
	// healthy peer as failed on every follower.
	PeerUnknown PeerHealth = iota
	// PeerUp means the most recent exchange succeeded.
	PeerUp
	// PeerDown means the most recent exchange failed.
	PeerDown
)

func (h PeerHealth) String() string {
	switch h {
	case PeerUp:
		return "up"
	case PeerDown:
		return "down"
	default:
		return "unknown"
	}
}

// GRPCTransport is the production transport.
type GRPCTransport struct {
	self    raft.NodeID
	handler Handler
	// dialTimeout bounds how long a send waits on a peer that is unreachable.
	// Without a bound, a partitioned peer would tie up a sender goroutine
	// indefinitely.
	dialTimeout time.Duration
	sendTimeout time.Duration
	maxInflight int

	mu    sync.RWMutex
	peers map[raft.NodeID]*grpcPeer

	closed bool
	wg     sync.WaitGroup
}

type grpcPeer struct {
	id   raft.NodeID
	addr string

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client raftv1.RaftServiceClient
	// health tracks the last observed outcome, reported through peer status so an
	// operator can see which links are down and which were never tried.
	health PeerHealth
	// inflight caps concurrent sends to one peer. A leader replicating to a slow
	// follower must not accumulate unbounded goroutines and message copies.
	inflight chan struct{}
}

// GRPCOptions configures a GRPCTransport.
type GRPCOptions struct {
	Self        raft.NodeID
	Handler     Handler
	DialTimeout time.Duration
	SendTimeout time.Duration
	// MaxInflightPerPeer bounds concurrent sends to a single peer. Defaults to 64.
	MaxInflightPerPeer int
}

// NewGRPCTransport creates a transport that dials peers lazily.
func NewGRPCTransport(opts GRPCOptions) *GRPCTransport {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 2 * time.Second
	}
	if opts.SendTimeout <= 0 {
		opts.SendTimeout = 2 * time.Second
	}
	if opts.MaxInflightPerPeer <= 0 {
		opts.MaxInflightPerPeer = 64
	}
	return &GRPCTransport{
		self:        opts.Self,
		handler:     opts.Handler,
		dialTimeout: opts.DialTimeout,
		sendTimeout: opts.SendTimeout,
		peers:       make(map[raft.NodeID]*grpcPeer),
		maxInflight: opts.MaxInflightPerPeer,
	}
}

// AddPeer registers a peer address, replacing any previous one.
func (t *GRPCTransport) AddPeer(id raft.NodeID, addr string) {
	if id == t.self {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.peers[id]; ok {
		if existing.addr == addr {
			return
		}
		// The address changed, so the old connection points somewhere no longer
		// authoritative for this id and must be dropped.
		existing.close()
	}
	t.peers[id] = &grpcPeer{id: id, addr: addr, inflight: make(chan struct{}, t.maxInflight)}
}

// RemovePeer drops a peer and closes its connection.
func (t *GRPCTransport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	p, ok := t.peers[id]
	delete(t.peers, id)
	t.mu.Unlock()
	if ok {
		p.close()
	}
}

// Peers returns the known peer addresses.
func (t *GRPCTransport) Peers() map[raft.NodeID]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[raft.NodeID]string, len(t.peers))
	for id, p := range t.peers {
		out[id] = p.addr
	}
	return out
}

// Health reports the last known state of the link to a peer.
func (t *GRPCTransport) Health(id raft.NodeID) PeerHealth {
	t.mu.RLock()
	p, ok := t.peers[id]
	t.mu.RUnlock()
	if !ok {
		return PeerUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.health
}

// Send delivers a message asynchronously.
func (t *GRPCTransport) Send(m raft.Message) {
	t.mu.RLock()
	p, ok := t.peers[m.To]
	closed := t.closed
	t.mu.RUnlock()
	if !ok || closed {
		return
	}

	select {
	case p.inflight <- struct{}{}:
	default:
		// The peer is already saturated. Dropping is correct: the protocol will
		// resend, and queueing without bound would turn one slow follower into
		// unbounded memory growth on the leader.
		return
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer func() { <-p.inflight }()
		t.exchange(p, m)
	}()
}

// exchange performs one request/response round trip and feeds any response back
// into the core as an inbound message.
//
// Mapping request/response RPCs onto the core's one-way message model happens
// here: from the core's point of view it sent a request and later received a
// reply, exactly as it would over a datagram transport.
func (t *GRPCTransport) exchange(p *grpcPeer, m raft.Message) {
	client, err := p.connect(t.dialTimeout)
	if err != nil {
		p.setHealth(PeerDown)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.sendTimeout)
	defer cancel()

	var reply raft.Message
	var haveReply bool

	switch m.Type {
	case raft.MsgVoteReq:
		resp, err := client.RequestVote(ctx, voteRequestToProto(m))
		if err != nil {
			p.setHealth(PeerDown)
			return
		}
		reply, haveReply = voteResponseFromProto(resp, t.self), true

	case raft.MsgAppReq:
		resp, err := client.AppendEntries(ctx, appendRequestToProto(m))
		if err != nil {
			p.setHealth(PeerDown)
			return
		}
		reply, haveReply = appendResponseFromProto(resp, t.self), true

	case raft.MsgSnapReq:
		resp, err := client.InstallSnapshot(ctx, snapshotRequestToProto(m))
		if err != nil {
			p.setHealth(PeerDown)
			return
		}
		reply, haveReply = snapshotResponseFromProto(resp, t.self), true

	case raft.MsgReadIndexReq:
		resp, err := client.ReadIndex(ctx, readIndexRequestToProto(m))
		if err != nil {
			p.setHealth(PeerDown)
			return
		}
		reply, haveReply = readIndexResponseFromProto(resp, t.self), true

	case raft.MsgTimeoutNow:
		if _, err := client.TimeoutNow(ctx, &raftv1.TimeoutNowRequest{
			Term: uint64(m.Term), LeaderId: uint64(m.From),
		}); err != nil {
			p.setHealth(PeerDown)
			return
		}

	case raft.MsgVoteResp, raft.MsgAppResp, raft.MsgSnapResp, raft.MsgReadIndexResp:
		// Responses travel back as RPC return values, so the core never asks the
		// transport to send one. Reaching here means the core and this switch have
		// drifted apart.
		return

	default:
		return
	}

	p.setHealth(PeerUp)
	if haveReply && t.handler != nil {
		t.handler(reply)
	}
}

// connect dials lazily and caches the connection.
func (p *grpcPeer) connect(timeout time.Duration) (raftv1.RaftServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	conn, err := grpc.NewClient(p.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", p.addr, err)
	}
	p.conn = conn
	p.client = raftv1.NewRaftServiceClient(conn)
	return p.client, nil
}

func (p *grpcPeer) setHealth(h PeerHealth) {
	p.mu.Lock()
	p.health = h
	p.mu.Unlock()
}

func (p *grpcPeer) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
		p.client = nil
	}
	p.health = PeerUnknown
}

// Close shuts the transport down, waiting for in-flight sends so that no
// goroutine outlives it.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	peers := make([]*grpcPeer, 0, len(t.peers))
	for _, p := range t.peers {
		peers = append(peers, p)
	}
	t.mu.Unlock()

	t.wg.Wait()
	for _, p := range peers {
		p.close()
	}
	return nil
}

// PeerServer implements the peer-facing RaftService, translating inbound RPCs
// into core messages and returning the core's reply.
//
// The reply must be produced synchronously, so the driver exposes a request/reply
// entry point rather than a fire-and-forget one for this direction.
type PeerServer struct {
	raftv1.UnimplementedRaftServiceServer
	self raft.NodeID
	// step delivers a message and waits for the response the core generates for
	// that sender, or returns false if none was produced.
	step func(context.Context, raft.Message) (raft.Message, bool)
}

// NewPeerServer wires a gRPC service onto a driver's step function.
func NewPeerServer(self raft.NodeID, step func(context.Context, raft.Message) (raft.Message, bool)) *PeerServer {
	return &PeerServer{self: self, step: step}
}

func (s *PeerServer) RequestVote(ctx context.Context, r *raftv1.VoteRequest) (*raftv1.VoteResponse, error) {
	reply, ok := s.step(ctx, voteRequestFromProto(r, s.self))
	if !ok {
		// No reply means the message was stale enough to be dropped. Refusing is
		// the safe answer: it can never cause the caller to win an election it
		// should not.
		return &raftv1.VoteResponse{VoterId: uint64(s.self), Granted: false, PreVote: r.GetPreVote()}, nil
	}
	return voteResponseToProto(reply), nil
}

func (s *PeerServer) AppendEntries(ctx context.Context, r *raftv1.AppendRequest) (*raftv1.AppendResponse, error) {
	reply, ok := s.step(ctx, appendRequestFromProto(r, s.self))
	if !ok {
		return &raftv1.AppendResponse{FollowerId: uint64(s.self), Reject: true, ReadCtx: r.GetReadCtx()}, nil
	}
	return appendResponseToProto(reply), nil
}

func (s *PeerServer) InstallSnapshot(ctx context.Context, r *raftv1.SnapshotRequest) (*raftv1.SnapshotResponse, error) {
	reply, ok := s.step(ctx, snapshotRequestFromProto(r, s.self))
	if !ok {
		return &raftv1.SnapshotResponse{FollowerId: uint64(s.self), Reject: true}, nil
	}
	return snapshotResponseToProto(reply), nil
}

func (s *PeerServer) ReadIndex(ctx context.Context, r *raftv1.ReadIndexRequest) (*raftv1.ReadIndexResponse, error) {
	reply, ok := s.step(ctx, readIndexRequestFromProto(r, s.self))
	if !ok {
		return &raftv1.ReadIndexResponse{FromId: uint64(s.self), Reject: true, ReadCtx: r.GetReadCtx()}, nil
	}
	return readIndexResponseToProto(reply), nil
}

func (s *PeerServer) TimeoutNow(ctx context.Context, r *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	s.step(ctx, raft.Message{
		Type: raft.MsgTimeoutNow,
		From: raft.NodeID(r.GetLeaderId()),
		To:   s.self,
		Term: raft.Term(r.GetTerm()),
	})
	return &raftv1.TimeoutNowResponse{}, nil
}

// Register attaches the service to a gRPC server.
func (s *PeerServer) Register(srv *grpc.Server) {
	raftv1.RegisterRaftServiceServer(srv, s)
}
