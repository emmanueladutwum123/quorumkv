package server

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
)

// End-to-end numbers: a real three-node cluster over loopback gRPC, with real
// fsyncs to a temporary directory. These are the only benchmarks here that
// include everything a client actually waits for -- serialisation, the network,
// a quorum, and the disk -- so they are the ones worth quoting, and the ones
// that show which of those four dominates.

// benchTick is short enough that a commit does not sit waiting on the next
// heartbeat. At the tests' default of 20ms that wait dominates every number
// here, and the benchmark would be measuring the harness rather than the store.
const benchTick = 2 * time.Millisecond

// BenchmarkClusterPut is the write path end to end. Sequential, so it measures
// latency rather than peak throughput: what one client waits for.
//
// What it showed, and worth knowing before optimising anything else here: a
// committed write costs two durable syncs on the leader, not one. The first
// makes the entry durable, which Raft requires before the entry may be
// acknowledged to anyone. The second carries only the advanced commit index,
// which Raft does *not* require to be durable -- a restarted node relearns it
// from the leader -- and it lands before the client is answered. On hardware
// where a sync costs milliseconds, that is a large fraction of the latency
// below, spent on a guarantee the protocol does not need.
func BenchmarkClusterPut(b *testing.B) {
	c := newTestCluster(b, 3, withTick(benchTick))
	kv, done := c.leaderClient()
	defer done()

	value := make([]byte, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := rpcCtx()
		_, err := kv.Put(ctx, &kvv1.PutRequest{
			Header: &kvv1.RequestHeader{ClientId: 1, Sequence: uint64(i + 1)},
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Value:  value,
		})
		cancel()
		if err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

// BenchmarkClusterPutParallel measures throughput instead: many clients in
// flight, so proposals batch into shared Ready cycles and amortise the fsync
// that dominates the sequential case.
func BenchmarkClusterPutParallel(b *testing.B) {
	c := newTestCluster(b, 3, withTick(benchTick))
	kv, done := c.leaderClient()
	defer done()

	value := make([]byte, 128)
	var client uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine is its own client, since the session table deduplicates
		// by (client, sequence) and a shared id would make every write after the
		// first look like a retry.
		id := atomic.AddUint64(&client, 1)
		seq := uint64(0)
		for pb.Next() {
			seq++
			ctx, cancel := rpcCtx()
			_, err := kv.Put(ctx, &kvv1.PutRequest{
				Header: &kvv1.RequestHeader{ClientId: id, Sequence: seq},
				Key:    []byte(fmt.Sprintf("key-%d-%d", id, seq)),
				Value:  value,
			})
			cancel()
			if err != nil {
				b.Errorf("put: %v", err)
				return
			}
		}
	})
}

// BenchmarkClusterReadLinearizable pays for leadership confirmation on every
// read: a round trip to a quorum, but no log append and no fsync.
func BenchmarkClusterReadLinearizable(b *testing.B) {
	benchRead(b, kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE)
}

// BenchmarkClusterReadStale skips that round trip and answers from local state.
// The gap between the two is exactly what linearizability costs a reader, and
// the reason both modes are offered.
func BenchmarkClusterReadStale(b *testing.B) {
	benchRead(b, kvv1.ConsistencyLevel_CONSISTENCY_LEVEL_STALE)
}

func benchRead(b *testing.B, level kvv1.ConsistencyLevel) {
	b.Helper()
	c := newTestCluster(b, 3, withTick(benchTick))
	c.putWithRetry("bench-key", "bench-value", 1, 1)

	kv, done := c.leaderClient()
	defer done()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := rpcCtx()
		got, err := kv.Get(ctx, &kvv1.GetRequest{
			Key:         []byte("bench-key"),
			Consistency: level,
		})
		cancel()
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if !got.GetFound() {
			b.Fatal("key missing")
		}
	}
}

// BenchmarkStatusSnapshot measures the round trip onto the consensus goroutine
// that a metrics scrape performs. It shares that goroutine with replication, so
// a scrape interval is a decision about how often to interrupt the hot loop.
func BenchmarkStatusSnapshot(b *testing.B) {
	c := newTestCluster(b, 3, withTick(benchTick))
	leader := c.waitForLeader(10 * time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := rpcCtx()
		if _, err := leader.srv.Status(ctx); err != nil {
			b.Fatalf("status: %v", err)
		}
		cancel()
	}
}
