// Package server wires the consensus core to durable storage, the network and
// the state machine, and exposes the client API.
//
// It is the driver: the component that honours the Ready contract. The core
// decides *what* must be durable and *when* messages may be sent; this package
// actually does it, in the required order.
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
	"github.com/emmanueladutwum123/quorumkv/internal/store"
	"github.com/emmanueladutwum123/quorumkv/internal/transport"
	"github.com/emmanueladutwum123/quorumkv/internal/wal"
)

// Errors surfaced to clients.
var (
	// ErrNotLeader means this node cannot serve the request. It carries the
	// leader's address when one is known, so a client is redirected in one hop
	// rather than polling the cluster.
	ErrNotLeader = errors.New("not leader")
	// ErrLeadershipLost means a proposal was accepted by a leader that was then
	// deposed before the entry committed. The outcome is genuinely unknown: the
	// entry may or may not survive, so the client must retry with the same
	// sequence number and let the session table deduplicate.
	ErrLeadershipLost = errors.New("leadership lost before the entry committed")
	// ErrTimeout means the request did not resolve within its deadline.
	ErrTimeout = errors.New("request timed out")
	// ErrStopped means the node is shutting down.
	ErrStopped = errors.New("node is stopped")
)

// NotLeaderError carries the current leader's address alongside ErrNotLeader.
type NotLeaderError struct {
	LeaderID   raft.NodeID
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "not leader, and no leader is currently known"
	}
	return fmt.Sprintf("not leader; leader is node %d at %s", e.LeaderID, e.LeaderAddr)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// Config configures a node.
type Config struct {
	NodeID raft.NodeID
	// Addr is the listen address serving both the peer and client APIs.
	Addr string
	// DataDir holds the write-ahead log and snapshots.
	DataDir string
	// Peers is the initial cluster, including this node.
	Peers map[raft.NodeID]string

	// TickInterval is the wall-clock period of one logical tick.
	TickInterval time.Duration
	// ElectionTimeoutTicks and HeartbeatTimeoutTicks are expressed in ticks, so
	// timing is configured in one place and the core stays clock-free.
	ElectionTimeoutTicks  int
	HeartbeatTimeoutTicks int

	// SnapshotThreshold is how many entries may accumulate past the compaction
	// boundary before a snapshot is taken. It trades recovery time against
	// snapshot frequency: a low value keeps restarts fast but snapshots often.
	SnapshotThreshold uint64

	// RequestTimeout bounds how long a client request waits to resolve.
	RequestTimeout time.Duration

	// Learner marks this node as non-voting on first start.
	Learner bool
}

func (c *Config) withDefaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeoutTicks <= 0 {
		c.ElectionTimeoutTicks = 10
	}
	if c.HeartbeatTimeoutTicks <= 0 {
		c.HeartbeatTimeoutTicks = 1
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 10000
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 5 * time.Second
	}
}

// Server is one cluster node.
//
// The consensus core is not safe for concurrent use, and rather than wrap it in a
// mutex the server confines it to a single goroutine. Everything else — client
// RPCs, peer RPCs, the ticker — hands work to that goroutine over channels.
//
// The reason is not performance but reasoning: with one owner, the sequence of
// operations the core sees is a single well-defined order, which is the same
// property the deterministic simulator relies on. A mutex would give mutual
// exclusion without giving that order.
type Server struct {
	cfg   Config
	node  *raft.Node
	wal   *wal.Log
	snaps *wal.SnapshotStore
	fsm   *store.Store
	tr    transport.Transport

	// Work queued for the single owning goroutine.
	stepCh    chan *stepRequest
	proposeCh chan *proposeRequest
	readCh    chan *readRequest
	statusCh  chan chan raft.Status

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// proposals maps a log index to the client waiting on it.
	proposals map[raft.Index]*pendingProposal
	// reads maps a read token to the client waiting on it.
	reads      map[uint64]*pendingRead
	nextReadID atomic.Uint64

	// lastSnapshot is the most recent snapshot taken, served to the core when it
	// needs to catch up a follower behind the compaction boundary.
	snapMu       sync.RWMutex
	lastSnapshot *raft.Snapshot

	// peerAddrs is read by client-facing code to answer redirects, so it is kept
	// behind its own lock rather than living only inside the loop goroutine.
	addrMu    sync.RWMutex
	peerAddrs map[raft.NodeID]string

	// Observable counters, useful for tests and metrics.
	metrics Metrics
}

// Metrics are cheap counters describing what the node has done.
type Metrics struct {
	Proposals        atomic.Uint64
	ProposalsLost    atomic.Uint64
	CommittedEntries atomic.Uint64
	AppliedEntries   atomic.Uint64
	Snapshots        atomic.Uint64
	SnapshotsApplied atomic.Uint64
	LinearizedReads  atomic.Uint64
	RejectedReads    atomic.Uint64
	Elections        atomic.Uint64
	FsyncCount       atomic.Uint64
}

type stepRequest struct {
	msg raft.Message
	// expect is the response type this caller is waiting for, or zero if none.
	expect   raft.MessageType
	wantResp bool
	replyCh  chan raft.Message
}

type proposeRequest struct {
	entryType raft.EntryType
	data      []byte
	replyCh   chan proposeResult
}

type proposeResult struct {
	index  raft.Index
	result *kvv1.CommandResult
	err    error
}

type pendingProposal struct {
	// term guards against a subtle failure: another leader may commit a different
	// entry at this index. Recording the term the proposal was accepted in lets
	// the driver notice and fail the client rather than reporting someone else's
	// entry as its result.
	term    raft.Term
	replyCh chan proposeResult
}

type readRequest struct {
	replyCh chan readResult
}

type readResult struct {
	index raft.Index
	err   error
}

type pendingRead struct {
	// index is the commit index this read must observe before it may be served.
	index    raft.Index
	resolved bool
	replyCh  chan readResult
}

// New creates a node, recovering any state left by a previous run.
func New(cfg Config) (*Server, error) {
	cfg.withDefaults()
	if cfg.NodeID == 0 {
		return nil, errors.New("server: NodeID is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("server: DataDir is required")
	}

	s := &Server{
		cfg:       cfg,
		fsm:       store.New(),
		stepCh:    make(chan *stepRequest, 256),
		proposeCh: make(chan *proposeRequest, 256),
		readCh:    make(chan *readRequest, 256),
		statusCh:  make(chan chan raft.Status, 16),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		proposals: make(map[raft.Index]*pendingProposal),
		reads:     make(map[uint64]*pendingRead),
		peerAddrs: make(map[raft.NodeID]string, len(cfg.Peers)),
	}
	for id, addr := range cfg.Peers {
		s.peerAddrs[id] = addr
	}

	// Recovery order matters and mirrors the durability argument: open the log
	// (which discards any torn tail), load the snapshot to establish the
	// compaction boundary, replay the entries that follow it, then adopt the
	// persisted term and vote.
	logDir := cfg.DataDir + "/wal"
	snapDir := cfg.DataDir + "/snapshots"

	l, recovered, err := wal.Open(wal.Options{Dir: logDir})
	if err != nil {
		return nil, fmt.Errorf("server: open wal: %w", err)
	}
	s.wal = l

	snaps, err := wal.NewSnapshotStore(snapDir)
	if err != nil {
		l.Close()
		return nil, fmt.Errorf("server: open snapshot store: %w", err)
	}
	s.snaps = snaps

	var initial raft.Config
	if len(cfg.Peers) > 0 {
		voters := make([]raft.NodeID, 0, len(cfg.Peers))
		for id := range cfg.Peers {
			if cfg.Learner && id == cfg.NodeID {
				continue
			}
			voters = append(voters, id)
		}
		initial = raft.NewConfig(voters...)
		if cfg.Learner {
			initial.Learners = raft.NewNodeSet(cfg.NodeID)
		}
	} else {
		initial = raft.NewConfig(cfg.NodeID)
	}

	node, err := raft.NewNode(raft.Options{
		ID:                    cfg.NodeID,
		Config:                initial,
		ElectionTimeoutTicks:  cfg.ElectionTimeoutTicks,
		HeartbeatTimeoutTicks: cfg.HeartbeatTimeoutTicks,
		Snapshots:             s,
	})
	if err != nil {
		l.Close()
		return nil, fmt.Errorf("server: create node: %w", err)
	}
	s.node = node

	if recovered.SnapshotIndex > 0 {
		snap, err := snaps.Latest()
		if err != nil {
			l.Close()
			return nil, fmt.Errorf("server: the log references a snapshot that cannot be read: %w", err)
		}
		if err := node.RestoreSnapshot(snap); err != nil && !errors.Is(err, raft.ErrSnapshotOutOfDate) {
			l.Close()
			return nil, fmt.Errorf("server: restore snapshot: %w", err)
		}
		if err := s.fsm.Restore(snap.Data, snap.Index); err != nil {
			l.Close()
			return nil, fmt.Errorf("server: restore state machine: %w", err)
		}
		s.setLastSnapshot(&snap)
	}
	node.ReplayEntries(recovered.Entries)
	node.SetHardState(recovered.HardState)

	s.tr = transport.NewGRPCTransport(transport.GRPCOptions{
		Self:    cfg.NodeID,
		Handler: s.deliver,
	})
	for id, addr := range cfg.Peers {
		if id != cfg.NodeID {
			s.tr.AddPeer(id, addr)
		}
	}
	return s, nil
}

// Metrics returns the node's counters.
func (s *Server) Metrics() *Metrics { return &s.metrics }

// FSM exposes the state machine, for read paths and tests.
func (s *Server) FSM() *store.Store { return s.fsm }

// Snapshot implements raft.SnapshotProvider.
//
// It returns the most recent snapshot already taken rather than building one on
// demand. Snapshotting is the state machine's decision and happens on the apply
// path; producing one here, mid-replication, would serialise a large copy inside
// the loop that also has to keep answering heartbeats.
func (s *Server) Snapshot() (raft.Snapshot, error) {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	if s.lastSnapshot == nil {
		return raft.Snapshot{}, raft.ErrUnavailable
	}
	return *s.lastSnapshot, nil
}

func (s *Server) setLastSnapshot(snap *raft.Snapshot) {
	s.snapMu.Lock()
	s.lastSnapshot = snap
	s.snapMu.Unlock()
}

// deliver hands a message arriving from a peer to the loop goroutine. It never
// blocks: the transport's callback runs on a gRPC handler goroutine, and blocking
// it would let one slow peer stall the node.
func (s *Server) deliver(m raft.Message) {
	select {
	case s.stepCh <- &stepRequest{msg: m}:
	case <-s.stopCh:
	default:
		// The queue is full, which means the node is already behind. Dropping is
		// what the protocol expects of an unreliable network, and it is strictly
		// better than growing without bound.
	}
}

// StepAndWait delivers a peer message and returns the reply the core produced,
// which is what the peer-facing gRPC handlers return to their callers.
func (s *Server) StepAndWait(ctx context.Context, m raft.Message) (raft.Message, bool) {
	expect, wants := responseType(m.Type)
	req := &stepRequest{msg: m, expect: expect, wantResp: wants}
	if wants {
		req.replyCh = make(chan raft.Message, 1)
	}

	select {
	case s.stepCh <- req:
	case <-ctx.Done():
		return raft.Message{}, false
	case <-s.stopCh:
		return raft.Message{}, false
	}

	if !wants {
		return raft.Message{}, false
	}
	select {
	case reply := <-req.replyCh:
		return reply, true
	case <-ctx.Done():
		return raft.Message{}, false
	case <-s.stopCh:
		return raft.Message{}, false
	}
}

// responseType maps a request to the reply the core will generate for it.
func responseType(t raft.MessageType) (raft.MessageType, bool) {
	switch t {
	case raft.MsgVoteReq:
		return raft.MsgVoteResp, true
	case raft.MsgAppReq:
		return raft.MsgAppResp, true
	case raft.MsgSnapReq:
		return raft.MsgSnapResp, true
	case raft.MsgReadIndexReq:
		return raft.MsgReadIndexResp, true
	default:
		return 0, false
	}
}

// Run drives the node until Stop is called. It owns the consensus core: no other
// goroutine touches it.
func (s *Server) Run() error {
	defer close(s.doneCh)

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			s.failAllWaiters(ErrStopped)
			return nil

		case <-ticker.C:
			s.node.Tick()

		case req := <-s.stepCh:
			s.handleStep(req)

		case req := <-s.proposeCh:
			s.handlePropose(req)

		case req := <-s.readCh:
			s.handleRead(req)

		case replyCh := <-s.statusCh:
			replyCh <- s.node.Status()
		}

		if err := s.drainReady(nil); err != nil {
			// A durability failure is not recoverable in place. Continuing would
			// mean acknowledging writes the node cannot honour, so it stops.
			s.failAllWaiters(err)
			return err
		}
	}
}

func (s *Server) handleStep(req *stepRequest) {
	if err := s.node.Step(req.msg); err != nil {
		// A malformed message is the sender's problem; the node carries on.
		if req.wantResp {
			close(req.replyCh)
		}
		return
	}
	if !req.wantResp {
		return
	}
	// Capture the reply this step generated for this sender rather than sending it
	// over the transport: with request/response RPCs, the reply *is* the return
	// value. Because Step and this drain happen inside one loop iteration, the
	// capture cannot be confused with a concurrent exchange.
	capture := &replyCapture{to: req.msg.From, typ: req.expect, ch: req.replyCh}
	if err := s.drainReady(capture); err != nil {
		s.failAllWaiters(err)
		return
	}
	if !capture.sent {
		// The core chose not to answer, which happens for a message stale enough to
		// be dropped. The handler turns this into a rejection.
		close(req.replyCh)
	}
}

type replyCapture struct {
	to   raft.NodeID
	typ  raft.MessageType
	ch   chan raft.Message
	sent bool
}

func (s *Server) handlePropose(req *proposeRequest) {
	index, err := s.node.ProposeEntry(req.entryType, req.data)
	if err != nil {
		req.replyCh <- proposeResult{err: s.notLeaderError()}
		return
	}
	s.metrics.Proposals.Add(1)
	// The entry is registered against the term it was accepted in, so that an
	// entry from a different leader landing at the same index is recognised as a
	// lost proposal rather than reported as this one's result.
	s.proposals[index] = &pendingProposal{term: s.node.Term(), replyCh: req.replyCh}
}

func (s *Server) handleRead(req *readRequest) {
	if s.node.Role() != raft.Leader {
		req.replyCh <- readResult{err: s.notLeaderError()}
		return
	}
	id := s.nextReadID.Add(1)
	ctx := make([]byte, 8)
	binary.BigEndian.PutUint64(ctx, id)
	s.reads[id] = &pendingRead{replyCh: req.replyCh}
	s.node.ReadIndex(ctx)
}

// drainReady performs the driver's side of the Ready contract, in the order the
// contract requires.
func (s *Server) drainReady(capture *replyCapture) error {
	for s.node.HasReady() {
		rd := s.node.Ready()

		// 1. Persist. Nothing may be sent or applied before this returns.
		if rd.Snapshot != nil {
			if err := s.persistSnapshot(rd.Snapshot); err != nil {
				return err
			}
		}
		var hs raft.HardState
		if rd.HardState != nil {
			hs = *rd.HardState
		}
		if len(rd.Entries) > 0 || rd.HardState != nil {
			if err := s.wal.Append(rd.Entries, hs); err != nil {
				return fmt.Errorf("server: append to wal: %w", err)
			}
			if err := s.wal.Sync(); err != nil {
				return fmt.Errorf("server: sync wal: %w", err)
			}
			s.metrics.FsyncCount.Add(1)
		}

		// 2. Send, now that the promises those messages make are durable.
		for _, m := range rd.Messages {
			if capture != nil && !capture.sent && m.To == capture.to && m.Type == capture.typ {
				capture.sent = true
				capture.ch <- m
				continue
			}
			if isResponse(m.Type) {
				// Responses exist only as RPC return values. One with nobody waiting
				// belongs to a call that has already timed out.
				continue
			}
			s.tr.Send(m)
		}

		// 3. Apply, then release reads that are now safe.
		if rd.SoftState != nil && rd.SoftState.Role == raft.Leader {
			s.metrics.Elections.Add(1)
		}
		if rd.SoftState != nil && rd.SoftState.Role != raft.Leader {
			// This node is no longer leader, so it can no longer promise anything
			// about proposals it accepted. Failing them immediately is better than
			// letting clients wait for a deadline that cannot be met.
			s.failProposals(ErrLeadershipLost)
		}
		if len(rd.Committed) > 0 {
			s.metrics.CommittedEntries.Add(uint64(len(rd.Committed)))
			if err := s.applyEntries(rd.Committed); err != nil {
				return err
			}
		}
		for _, rs := range rd.ReadStates {
			s.resolveRead(rs)
		}
		s.releaseReads()

		// 4. Acknowledge.
		s.node.Advance(rd)

		if err := s.maybeSnapshot(); err != nil {
			return err
		}
	}
	return nil
}

func isResponse(t raft.MessageType) bool {
	switch t {
	case raft.MsgVoteResp, raft.MsgAppResp, raft.MsgSnapResp, raft.MsgReadIndexResp:
		return true
	default:
		return false
	}
}

func (s *Server) persistSnapshot(snap *raft.Snapshot) error {
	if err := s.snaps.Save(*snap); err != nil {
		return fmt.Errorf("server: save snapshot: %w", err)
	}
	if err := s.wal.MarkSnapshot(snap.Index, snap.Term); err != nil {
		return fmt.Errorf("server: mark snapshot: %w", err)
	}
	if err := s.fsm.Restore(snap.Data, snap.Index); err != nil {
		return fmt.Errorf("server: restore state machine from snapshot: %w", err)
	}
	if _, err := s.wal.PurgeUpTo(snap.Index); err != nil {
		return fmt.Errorf("server: purge wal: %w", err)
	}
	s.setLastSnapshot(snap)
	s.metrics.SnapshotsApplied.Add(1)
	// Entries the snapshot covers can never commit for a waiting client now.
	s.failProposalsBelow(snap.Index, ErrLeadershipLost)
	return nil
}

func (s *Server) applyEntries(ents []raft.Entry) error {
	for _, e := range ents {
		result, err := s.fsm.Apply(e)
		if err != nil {
			// A command the state machine cannot apply would make this replica
			// diverge from the others. Stopping is the only safe response.
			return fmt.Errorf("server: apply entry %d: %w", e.Index, err)
		}
		s.metrics.AppliedEntries.Add(1)

		waiter, ok := s.proposals[e.Index]
		if !ok {
			continue
		}
		delete(s.proposals, e.Index)
		if waiter.term != e.Term {
			// A different leader's entry occupies this index, so the client's
			// proposal was overwritten and never committed.
			s.metrics.ProposalsLost.Add(1)
			waiter.replyCh <- proposeResult{err: ErrLeadershipLost}
			continue
		}
		waiter.replyCh <- proposeResult{index: e.Index, result: result}
	}
	return nil
}

// resolveRead records the index a read must observe, or fails it outright.
func (s *Server) resolveRead(rs raft.ReadState) {
	if len(rs.ReadCtx) != 8 {
		return
	}
	id := binary.BigEndian.Uint64(rs.ReadCtx)
	pending, ok := s.reads[id]
	if !ok {
		return
	}
	if rs.Index == 0 {
		// The core could not prove leadership, so this read cannot be served
		// linearizably. Refusing is the whole point of the mechanism.
		delete(s.reads, id)
		s.metrics.RejectedReads.Add(1)
		pending.replyCh <- readResult{err: s.notLeaderError()}
		return
	}
	pending.index = rs.Index
	pending.resolved = true
}

// releaseReads serves every read whose index the state machine has now applied.
func (s *Server) releaseReads() {
	if len(s.reads) == 0 {
		return
	}
	applied := s.node.AppliedIndex()
	for id, pending := range s.reads {
		if !pending.resolved || pending.index > applied {
			continue
		}
		delete(s.reads, id)
		s.metrics.LinearizedReads.Add(1)
		pending.replyCh <- readResult{index: pending.index}
	}
}

// maybeSnapshot compacts once enough entries have accumulated past the boundary.
func (s *Server) maybeSnapshot() error {
	applied := s.node.AppliedIndex()
	boundary := s.node.SnapshotIndex()
	if applied <= boundary || uint64(applied-boundary) < s.cfg.SnapshotThreshold {
		return nil
	}

	data, err := s.fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("server: take snapshot: %w", err)
	}
	term, err := s.node.TermAt(applied)
	if err != nil {
		// Without the term at this index the snapshot could not answer a leader's
		// consistency check at the boundary, so it is not worth taking.
		return nil
	}
	snap := raft.Snapshot{Index: applied, Term: term, Conf: s.node.Membership(), Data: data}

	if err := s.snaps.Save(snap); err != nil {
		return fmt.Errorf("server: save snapshot: %w", err)
	}
	if err := s.wal.MarkSnapshot(snap.Index, snap.Term); err != nil {
		return fmt.Errorf("server: mark snapshot: %w", err)
	}
	// The snapshot is durable before the log prefix is released, never the other
	// way round: releasing first would leave a window in which neither holds the
	// compacted entries.
	if err := s.node.Compact(snap.Index); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return fmt.Errorf("server: compact: %w", err)
	}
	if _, err := s.wal.PurgeUpTo(snap.Index); err != nil {
		return fmt.Errorf("server: purge wal: %w", err)
	}
	s.setLastSnapshot(&snap)
	s.metrics.Snapshots.Add(1)
	return nil
}

func (s *Server) failProposals(err error) {
	for index, waiter := range s.proposals {
		delete(s.proposals, index)
		s.metrics.ProposalsLost.Add(1)
		waiter.replyCh <- proposeResult{err: err}
	}
}

func (s *Server) failProposalsBelow(index raft.Index, err error) {
	for i, waiter := range s.proposals {
		if i > index {
			continue
		}
		delete(s.proposals, i)
		s.metrics.ProposalsLost.Add(1)
		waiter.replyCh <- proposeResult{err: err}
	}
}

func (s *Server) failAllWaiters(err error) {
	s.failProposals(err)
	for id, pending := range s.reads {
		delete(s.reads, id)
		pending.replyCh <- readResult{err: err}
	}
}

// notLeaderError builds a redirect naming the current leader, if known.
func (s *Server) notLeaderError() error {
	leader := s.node.Leader()
	s.addrMu.RLock()
	addr := s.peerAddrs[leader]
	s.addrMu.RUnlock()
	return &NotLeaderError{LeaderID: leader, LeaderAddr: addr}
}

// PeerAddr returns the known address of a node.
func (s *Server) PeerAddr(id raft.NodeID) string {
	s.addrMu.RLock()
	defer s.addrMu.RUnlock()
	return s.peerAddrs[id]
}

// Status returns a snapshot of the node's consensus state.
func (s *Server) Status(ctx context.Context) (raft.Status, error) {
	replyCh := make(chan raft.Status, 1)
	select {
	case s.statusCh <- replyCh:
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
	case <-s.stopCh:
		return raft.Status{}, ErrStopped
	}
	select {
	case st := <-replyCh:
		return st, nil
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
	case <-s.stopCh:
		return raft.Status{}, ErrStopped
	}
}

// propose submits an entry and waits for it to be applied.
func (s *Server) propose(ctx context.Context, t raft.EntryType, data []byte) (raft.Index, *kvv1.CommandResult, error) {
	req := &proposeRequest{entryType: t, data: data, replyCh: make(chan proposeResult, 1)}

	select {
	case s.proposeCh <- req:
	case <-ctx.Done():
		return 0, nil, ErrTimeout
	case <-s.stopCh:
		return 0, nil, ErrStopped
	}

	select {
	case res := <-req.replyCh:
		return res.index, res.result, res.err
	case <-ctx.Done():
		// The entry may still commit. The client must retry with the same sequence
		// number and let the session table make that safe.
		return 0, nil, ErrTimeout
	case <-s.stopCh:
		return 0, nil, ErrStopped
	}
}

// readIndex waits until a linearizable read is safe to serve locally.
func (s *Server) readIndex(ctx context.Context) (raft.Index, error) {
	req := &readRequest{replyCh: make(chan readResult, 1)}

	select {
	case s.readCh <- req:
	case <-ctx.Done():
		return 0, ErrTimeout
	case <-s.stopCh:
		return 0, ErrStopped
	}

	select {
	case res := <-req.replyCh:
		return res.index, res.err
	case <-ctx.Done():
		return 0, ErrTimeout
	case <-s.stopCh:
		return 0, ErrStopped
	}
}

// Stop shuts the node down. It is safe to call more than once.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh

	var err error
	if s.tr != nil {
		err = s.tr.Close()
	}
	if s.wal != nil {
		if cerr := s.wal.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
