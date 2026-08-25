package raft

import (
	"fmt"
	"testing"
)

// The consensus core does no I/O and holds no locks, so what these measure is
// the algorithm itself: the cost of getting an entry from a proposal to a
// quorum, and the cost of the paths that run on every tick whether or not
// anything is happening.

// BenchmarkReplicationRoundTrip measures a full commit: propose on the leader,
// replicate to a quorum, and advance the commit index. This is the number that
// bounds throughput once the disk is taken out of the picture.
func BenchmarkReplicationRoundTrip(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			voters := make([]NodeID, size)
			for i := range voters {
				voters[i] = NodeID(i + 1)
			}
			c := newCluster(b, voters)
			leader := c.electLeader()
			payload := make([]byte, 128)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := leader.node.Propose(payload); err != nil {
					b.Fatalf("propose: %v", err)
				}
				c.run()
			}
		})
	}
}

// BenchmarkProposeBatch measures proposals accumulated before the network runs,
// which is what a busy leader actually does: entries pile up between Ready
// batches and replicate together.
func BenchmarkProposeBatch(b *testing.B) {
	for _, batch := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			c := newCluster(b, []NodeID{1, 2, 3})
			leader := c.electLeader()
			payload := make([]byte, 128)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < batch; j++ {
					if _, err := leader.node.Propose(payload); err != nil {
						b.Fatalf("propose: %v", err)
					}
				}
				c.run()
			}
			b.ReportMetric(float64(batch)*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
		})
	}
}

// BenchmarkTickIdle measures the per-tick cost on a leader with nothing to do.
// It runs on every node many times a second forever, so an allocation here is
// one the garbage collector sees for the life of the process.
func BenchmarkTickIdle(b *testing.B) {
	c := newCluster(b, []NodeID{1, 2, 3})
	leader := c.electLeader()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leader.node.Tick()
	}
}

// BenchmarkReadIndex measures the leadership confirmation a linearizable read
// waits on. Reads dominate most workloads, and this path is the reason they
// cost a round trip rather than a local map lookup.
func BenchmarkReadIndex(b *testing.B) {
	c := newCluster(b, []NodeID{1, 2, 3})
	leader := c.electLeader()
	ctx := []byte("bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		leader.node.ReadIndex(ctx)
		c.run()
	}
}

// BenchmarkStepAppendEntries measures the follower side in isolation: accepting
// a batch, checking the previous-entry match, and truncating nothing.
func BenchmarkStepAppendEntries(b *testing.B) {
	c := newCluster(b, []NodeID{1, 2, 3})
	c.electLeader()
	follower := c.node(2)

	term := follower.node.Status().Term
	payload := make([]byte, 128)
	next := follower.node.Status().LastLog + 1

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prev := next - 1
		prevTerm, err := logTermAt(follower, prev)
		if err != nil {
			b.Fatalf("term at %d: %v", prev, err)
		}
		err = follower.node.Step(Message{
			Type:         MsgAppReq,
			From:         1,
			To:           2,
			Term:         term,
			PrevLogIndex: prev,
			PrevLogTerm:  prevTerm,
			Entries:      []Entry{{Index: next, Term: term, Type: EntryNormal, Data: payload}},
		})
		if err != nil {
			b.Fatalf("step: %v", err)
		}
		b.StopTimer()
		follower.node.Advance(follower.node.Ready())
		next++
		b.StartTimer()
	}
}

func logTermAt(p *peer, index Index) (Term, error) {
	return p.node.log.term(index)
}
