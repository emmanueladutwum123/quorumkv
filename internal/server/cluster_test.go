package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
	"github.com/emmanueladutwum123/quorumkv/internal/transport"
)

// These are integration tests: real gRPC over loopback, real fsyncs to a temp
// directory, real elections on a real clock. They exist because the deterministic
// simulator deliberately cannot catch driver bugs — it substitutes for the
// network and the disk, which is exactly where this layer's mistakes live.

// testCluster is a set of in-process nodes talking over loopback.
type testCluster struct {
	t     testing.TB
	nodes map[raft.NodeID]*testNodeProc
	addrs map[raft.NodeID]string
	dir   string
}

type testNodeProc struct {
	id       raft.NodeID
	addr     string
	dataDir  string
	srv      *Server
	grpc     *grpc.Server
	listener net.Listener
	stopped  bool
}

// newTestCluster starts n nodes. Listeners are opened before the servers are
// created so that each node's address is known to every other from the start,
// without a discovery mechanism the test would otherwise have to fake.
func newTestCluster(t testing.TB, n int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:     t,
		nodes: make(map[raft.NodeID]*testNodeProc, n),
		addrs: make(map[raft.NodeID]string, n),
		dir:   t.TempDir(),
	}

	listeners := make(map[raft.NodeID]net.Listener, n)
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[id] = ln
		c.addrs[id] = ln.Addr().String()
	}

	for id, ln := range listeners {
		c.nodes[id] = &testNodeProc{
			id:       id,
			addr:     c.addrs[id],
			dataDir:  fmt.Sprintf("%s/node%d", c.dir, id),
			listener: ln,
		}
		c.start(id)
	}

	t.Cleanup(c.stopAll)
	return c
}

// start brings a node up on its existing listener, recovering any state on disk.
func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()
	n := c.nodes[id]

	peers := make(map[raft.NodeID]string, len(c.addrs))
	for pid, addr := range c.addrs {
		peers[pid] = addr
	}

	srv, err := New(Config{
		NodeID:  id,
		Addr:    n.addr,
		DataDir: n.dataDir,
		Peers:   peers,
		// Short ticks keep these tests fast while leaving the 10:1 ratio between
		// election and heartbeat timeouts that stops jitter looking like failure.
		TickInterval:          20 * time.Millisecond,
		ElectionTimeoutTicks:  10,
		HeartbeatTimeoutTicks: 1,
		SnapshotThreshold:     50,
		RequestTimeout:        3 * time.Second,
	})
	if err != nil {
		c.t.Fatalf("node %d: New: %v", id, err)
	}

	grpcServer := grpc.NewServer()
	transport.NewPeerServer(id, srv.StepAndWait).Register(grpcServer)
	NewKVService(srv).Register(grpcServer)

	n.srv = srv
	n.grpc = grpcServer
	n.stopped = false

	go func() { _ = srv.Run() }()
	go func() { _ = grpcServer.Serve(n.listener) }()
}

// restart stops a node and brings it back on the same address, recovering from
// disk alone. The listener is recreated on the same port so peers can reconnect
// without being told a new address.
func (c *testCluster) restart(id raft.NodeID) {
	c.t.Helper()
	c.stop(id)

	ln, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		c.t.Fatalf("relisten on %s: %v", c.addrs[id], err)
	}
	c.nodes[id].listener = ln
	c.start(id)
}

func (c *testCluster) stop(id raft.NodeID) {
	n := c.nodes[id]
	if n.stopped {
		return
	}
	n.stopped = true
	n.grpc.Stop()
	_ = n.srv.Stop()
}

func (c *testCluster) stopAll() {
	for id := range c.nodes {
		c.stop(id)
	}
}

// waitForLeader polls until exactly one node reports leadership.
func (c *testCluster) waitForLeader(timeout time.Duration) *testNodeProc {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*testNodeProc
		for _, n := range c.nodes {
			if n.stopped {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			st, err := n.srv.Status(ctx)
			cancel()
			if err == nil && st.Role == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			// More than one leader is only legitimate across different terms, where
			// the extra is a deposed leader that has not noticed yet. Two in the same
			// term is a safety violation.
			byTerm := map[raft.Term][]raft.NodeID{}
			for _, n := range leaders {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				st, err := n.srv.Status(ctx)
				cancel()
				if err == nil {
					byTerm[st.Term] = append(byTerm[st.Term], n.id)
				}
			}
			for term, ids := range byTerm {
				if len(ids) > 1 {
					c.t.Fatalf("SAFETY VIOLATION: nodes %v all claim leadership in term %d", ids, term)
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %s", timeout)
	return nil
}

// kvClient dials a node's client API.
func (c *testCluster) kvClient(id raft.NodeID) (kvv1.KVServiceClient, func()) {
	c.t.Helper()
	conn, err := grpc.NewClient(c.addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("dial node %d: %v", id, err)
	}
	return kvv1.NewKVServiceClient(conn), func() { conn.Close() }
}

// leaderClient dials whichever node currently leads.
func (c *testCluster) leaderClient() (kvv1.KVServiceClient, func()) {
	c.t.Helper()
	leader := c.waitForLeader(10 * time.Second)
	return c.kvClient(leader.id)
}

func rpcCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// putWithRetry writes through the leader, tolerating the leader changing under it.
func (c *testCluster) putWithRetry(key, value string, clientID, seq uint64) {
	c.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		kv, done := c.leaderClient()
		ctx, cancel := rpcCtx()
		_, err := kv.Put(ctx, &kvv1.PutRequest{
			Header: &kvv1.RequestHeader{ClientId: clientID, Sequence: seq},
			Key:    []byte(key), Value: []byte(value),
		})
		cancel()
		done()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("put %q failed after retries: %v", key, lastErr)
}

// --- tests -----------------------------------------------------------------

func TestClusterElectsLeaderAndServesWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	kv, done := c.kvClient(leader.id)
	defer done()

	ctx, cancel := rpcCtx()
	defer cancel()

	resp, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if resp.GetCommitIndex() == 0 {
		t.Error("Put returned commit index 0")
	}

	got, err := kv.Get(ctx, &kvv1.GetRequest{
		Key:         []byte("k"),
		Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || string(got.GetValue()) != "v" {
		t.Errorf("Get returned (found %v, %q), want (true, %q)", got.GetFound(), got.GetValue(), "v")
	}
}

func TestLinearizableReadObservesAcknowledgedWrite(t *testing.T) {
	// The defining property: once a write is acknowledged, no subsequent read may
	// fail to see it — including a read issued immediately afterwards.
	c := newTestCluster(t, 3)
	kv, done := c.leaderClient()
	defer done()

	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)

		ctx, cancel := rpcCtx()
		if _, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte(key), Value: []byte(value)}); err != nil {
			cancel()
			t.Fatalf("Put %s: %v", key, err)
		}
		got, err := kv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte(key),
			Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
		})
		cancel()
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if !got.GetFound() || string(got.GetValue()) != value {
			t.Fatalf("read immediately after writing %s returned (found %v, %q)",
				key, got.GetFound(), got.GetValue())
		}
	}
}

func TestFollowerRejectsWriteWithLeaderRedirect(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	var follower raft.NodeID
	for id := range c.nodes {
		if id != leader.id {
			follower = id
			break
		}
	}

	kv, done := c.kvClient(follower)
	defer done()
	ctx, cancel := rpcCtx()
	defer cancel()

	_, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if err == nil {
		t.Fatal("a follower accepted a write")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("error code = %v, want FailedPrecondition", status.Code(err))
	}
	// The redirect must name where to go, or a client is reduced to guessing.
	if want := c.addrs[leader.id]; !contains(st.Message(), want) {
		t.Errorf("redirect %q does not contain the leader address %q", st.Message(), want)
	}
}

func TestStaleReadIsServedByFollower(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	kv, done := c.kvClient(leader.id)
	ctx, cancel := rpcCtx()
	if _, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte("k"), Value: []byte("v")}); err != nil {
		cancel()
		done()
		t.Fatalf("Put: %v", err)
	}
	cancel()
	done()

	var follower raft.NodeID
	for id := range c.nodes {
		if id != leader.id {
			follower = id
			break
		}
	}
	fkv, fdone := c.kvClient(follower)
	defer fdone()

	// A stale read needs no coordination, so it must succeed on a follower — the
	// weaker guarantee is the whole reason to offer it.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := rpcCtx()
		got, err := fkv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte("k"),
			Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_STALE,
		})
		cancel()
		if err != nil {
			t.Fatalf("stale Get on follower: %v", err)
		}
		if got.GetFound() && string(got.GetValue()) == "v" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("the follower never caught up enough to serve the stale read")
}

func TestCommittedDataSurvivesLeaderFailure(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	for i := 0; i < 10; i++ {
		c.putWithRetry(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i), 1, uint64(i+1))
	}

	// Take the leader down and let the remaining majority elect a replacement.
	c.stop(leader.id)
	newLeader := c.waitForLeader(15 * time.Second)
	if newLeader.id == leader.id {
		t.Fatal("the stopped node is still reported as leader")
	}

	kv, done := c.kvClient(newLeader.id)
	defer done()
	for i := 0; i < 10; i++ {
		ctx, cancel := rpcCtx()
		got, err := kv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte(fmt.Sprintf("k%02d", i)),
			Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
		})
		cancel()
		if err != nil {
			t.Fatalf("Get after failover: %v", err)
		}
		if want := fmt.Sprintf("v%02d", i); !got.GetFound() || string(got.GetValue()) != want {
			t.Errorf("k%02d = (found %v, %q) after failover, want %q",
				i, got.GetFound(), got.GetValue(), want)
		}
	}
}

func TestRestartedNodeRecoversFromDisk(t *testing.T) {
	c := newTestCluster(t, 3)
	c.waitForLeader(10 * time.Second)

	for i := 0; i < 12; i++ {
		c.putWithRetry(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i), 2, uint64(i+1))
	}

	leader := c.waitForLeader(10 * time.Second)
	var victim raft.NodeID
	for id := range c.nodes {
		if id != leader.id {
			victim = id
			break
		}
	}

	// Everything volatile is discarded; only what was fsynced comes back.
	c.restart(victim)
	c.waitForLeader(15 * time.Second)

	kv, done := c.kvClient(victim)
	defer done()

	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := rpcCtx()
		got, err := kv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte("k11"),
			Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_STALE,
		})
		cancel()
		if err == nil && got.GetFound() && string(got.GetValue()) == "v11" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node never recovered k11 (last err: %v)", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestSnapshotAndCompactionUnderLoad(t *testing.T) {
	// SnapshotThreshold is 50 in these tests, so this many writes forces several
	// snapshot-and-compact cycles while the cluster is serving traffic.
	c := newTestCluster(t, 3)
	c.waitForLeader(10 * time.Second)

	for i := 0; i < 140; i++ {
		c.putWithRetry(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i), 3, uint64(i+1))
	}

	leader := c.waitForLeader(10 * time.Second)
	ctx, cancel := rpcCtx()
	st, err := leader.srv.Status(ctx)
	cancel()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Snapshot == 0 {
		t.Error("no compaction happened despite exceeding the snapshot threshold")
	}
	if leader.srv.Metrics().Snapshots.Load() == 0 {
		t.Error("the snapshot counter never advanced")
	}

	// Compaction must not cost correctness: every key written is still readable.
	kv, done := c.kvClient(leader.id)
	defer done()
	for _, i := range []int{0, 1, 70, 139} {
		ctx, cancel := rpcCtx()
		got, err := kv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte(fmt.Sprintf("k%03d", i)),
			Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
		})
		cancel()
		if err != nil {
			t.Fatalf("Get k%03d: %v", i, err)
		}
		if want := fmt.Sprintf("v%03d", i); !got.GetFound() || string(got.GetValue()) != want {
			t.Errorf("k%03d = (found %v, %q), want %q", i, got.GetFound(), got.GetValue(), want)
		}
	}
}

func TestCompareAndSwapSerialisesConcurrentClients(t *testing.T) {
	// Many clients race to create the same key. Exactly one may win, which is the
	// property that makes the store usable for coordination.
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	kv, done := c.kvClient(leader.id)
	defer done()

	const contenders = 12
	type outcome struct {
		swapped bool
		err     error
	}
	results := make(chan outcome, contenders)

	for i := 0; i < contenders; i++ {
		go func(i int) {
			ctx, cancel := rpcCtx()
			defer cancel()
			resp, err := kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
				Header:       &kvv1.RequestHeader{ClientId: uint64(100 + i), Sequence: 1},
				Key:          []byte("lock"),
				ExpectAbsent: true,
				NewValue:     []byte(fmt.Sprintf("client-%d", i)),
			})
			if err != nil {
				results <- outcome{err: err}
				return
			}
			results <- outcome{swapped: resp.GetSwapped()}
		}(i)
	}

	winners, failures := 0, 0
	for i := 0; i < contenders; i++ {
		res := <-results
		switch {
		case res.err != nil:
			failures++
		case res.swapped:
			winners++
		}
	}
	if winners != 1 {
		t.Errorf("%d clients acquired the lock (with %d RPC failures), want exactly 1", winners, failures)
	}
}

func TestRetriedWriteIsAppliedOnce(t *testing.T) {
	// A client that retries with the same sequence number must not have its
	// command applied twice, even though the entry reaches the log twice.
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)
	kv, done := c.kvClient(leader.id)
	defer done()

	ctx, cancel := rpcCtx()
	defer cancel()

	hdr := &kvv1.RequestHeader{ClientId: 42, Sequence: 1}
	first, err := kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
		Header: hdr, Key: []byte("once"), ExpectAbsent: true, NewValue: []byte("a"),
	})
	if err != nil {
		t.Fatalf("first CAS: %v", err)
	}
	if !first.GetSwapped() {
		t.Fatal("the first CAS did not swap")
	}

	// The same request again: a replay, which must return the original answer.
	retry, err := kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{
		Header: hdr, Key: []byte("once"), ExpectAbsent: true, NewValue: []byte("a"),
	})
	if err != nil {
		t.Fatalf("retried CAS: %v", err)
	}
	if !retry.GetSwapped() {
		t.Error("the retry reported failure; a replay must return the result the original produced")
	}
}

func TestMinorityCannotServeWritesOrLinearizableReads(t *testing.T) {
	// With a majority gone the cluster must refuse rather than answer from state
	// it cannot prove current. Refusing is the correct failure mode: unavailable,
	// not wrong.
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	c.putWithRetry("before", "v", 5, 1)

	// Stop both followers, leaving one node of three.
	var survivor raft.NodeID = leader.id
	for id := range c.nodes {
		if id != leader.id {
			c.stop(id)
		}
	}

	kv, done := c.kvClient(survivor)
	defer done()

	// Give the surviving node time to notice it cannot reach a majority.
	time.Sleep(1 * time.Second)

	ctx, cancel := rpcCtx()
	defer cancel()

	if _, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte("after"), Value: []byte("v")}); err == nil {
		t.Error("a single node out of three accepted a write")
	}
	if _, err := kv.Get(ctx, &kvv1.GetRequest{
		Key:         []byte("before"),
		Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	}); err == nil {
		t.Error("a single node out of three served a linearizable read")
	}
}

func TestSingleNodeClusterWorks(t *testing.T) {
	// A one-node cluster is its own quorum. It is the simplest way to try the
	// store out, so it must work rather than deadlock waiting for peers.
	c := newTestCluster(t, 1)
	leader := c.waitForLeader(10 * time.Second)

	kv, done := c.kvClient(leader.id)
	defer done()
	ctx, cancel := rpcCtx()
	defer cancel()

	if _, err := kv.Put(ctx, &kvv1.PutRequest{Key: []byte("solo"), Value: []byte("v")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := kv.Get(ctx, &kvv1.GetRequest{
		Key:         []byte("solo"),
		Consistency: kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || string(got.GetValue()) != "v" {
		t.Errorf("solo = (found %v, %q), want v", got.GetFound(), got.GetValue())
	}
}

func TestStatusReportsClusterView(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader(10 * time.Second)

	kv, done := c.kvClient(leader.id)
	defer done()
	ctx, cancel := rpcCtx()
	defer cancel()

	resp, err := kv.Status(ctx, &kvv1.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.GetRole() != "leader" {
		t.Errorf("role = %q, want leader", resp.GetRole())
	}
	if resp.GetNodeId() != uint64(leader.id) {
		t.Errorf("node id = %d, want %d", resp.GetNodeId(), leader.id)
	}
	if len(resp.GetPeers()) != 3 {
		t.Errorf("reported %d peers, want 3", len(resp.GetPeers()))
	}
	// A leader knows every peer's match index; that is what makes the command
	// useful for spotting a replica that has fallen behind.
	for _, p := range resp.GetPeers() {
		if p.GetAddress() == "" {
			t.Errorf("peer %d has no address", p.GetNodeId())
		}
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := New(Config{DataDir: t.TempDir()}); err == nil {
		t.Error("New accepted a zero NodeID")
	}
	if _, err := New(Config{NodeID: 1}); err == nil {
		t.Error("New accepted an empty DataDir")
	}
}

func TestNotLeaderErrorMatching(t *testing.T) {
	err := &NotLeaderError{LeaderID: 2, LeaderAddr: "127.0.0.1:9000"}
	if !errors.Is(err, ErrNotLeader) {
		t.Error("NotLeaderError does not match ErrNotLeader")
	}
	if !contains(err.Error(), "127.0.0.1:9000") {
		t.Errorf("error %q does not name the leader address", err)
	}
	// With no leader known the message must say so rather than name an empty
	// address a client would then try to dial.
	unknown := &NotLeaderError{}
	if contains(unknown.Error(), " at ") {
		t.Errorf("error %q implies an address when none is known", unknown)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		func() bool {
			for i := 0; i+len(needle) <= len(haystack); i++ {
				if haystack[i:i+len(needle)] == needle {
					return true
				}
			}
			return false
		}()
}
