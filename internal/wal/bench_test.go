package wal

import (
	"fmt"
	"testing"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// The write-ahead log is where a consensus system spends its latency, and almost
// all of it is in one place: the durable sync. These benchmarks exist to show
// where that cost lands and what amortises it, because the answer determines how
// the layer above should batch.

func benchEntries(start raft.Index, count, payload int) []raft.Entry {
	ents := make([]raft.Entry, count)
	data := make([]byte, payload)
	for i := range data {
		data[i] = byte(i)
	}
	for i := range ents {
		ents[i] = raft.Entry{
			Index: start + raft.Index(i),
			Term:  1,
			Type:  raft.EntryNormal,
			Data:  data,
		}
	}
	return ents
}

// BenchmarkAppendSync measures the cost of durability itself: one batch, one
// fsync, which is what a leader pays before it may acknowledge a write.
func BenchmarkAppendSync(b *testing.B) {
	for _, batch := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			log, _, err := Open(Options{Dir: b.TempDir()})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer func() { _ = log.Close() }()

			next := raft.Index(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ents := benchEntries(next, batch, 128)
				next += raft.Index(batch)
				if err := log.Append(ents, raft.HardState{Term: 1, Commit: next - 1}); err != nil {
					b.Fatalf("append: %v", err)
				}
				if err := log.Sync(); err != nil {
					b.Fatalf("sync: %v", err)
				}
			}
			// Entries per second is the number that matters, not batches per second:
			// a larger batch buys throughput by spreading one sync over more work.
			b.ReportMetric(float64(batch)*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
		})
	}
}

// BenchmarkAppendNoSync isolates the encoding and buffering from the sync, so
// the difference against the benchmark above is the price of durability.
func BenchmarkAppendNoSync(b *testing.B) {
	log, _, err := Open(Options{Dir: b.TempDir(), NoSync: true})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = log.Close() }()

	next := raft.Index(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ents := benchEntries(next, 1, 128)
		next++
		if err := log.Append(ents, raft.HardState{Term: 1, Commit: next - 1}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

// BenchmarkAppendPayloadSize shows how cost scales with entry size, which is
// what decides whether large values belong in the log at all.
func BenchmarkAppendPayloadSize(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			log, _, err := Open(Options{Dir: b.TempDir(), NoSync: true})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer func() { _ = log.Close() }()

			next := raft.Index(1)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ents := benchEntries(next, 1, size)
				next++
				if err := log.Append(ents, raft.HardState{Term: 1}); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
		})
	}
}

// BenchmarkOpenRecovery measures restart time, which is downtime: a node cannot
// vote or serve until it has read its log back.
func BenchmarkOpenRecovery(b *testing.B) {
	for _, count := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			dir := b.TempDir()
			log, _, err := Open(Options{Dir: dir, NoSync: true})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			if err := log.Append(benchEntries(1, count, 128), raft.HardState{Term: 1}); err != nil {
				b.Fatalf("append: %v", err)
			}
			if err := log.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reopened, _, err := Open(Options{Dir: dir, NoSync: true})
				if err != nil {
					b.Fatalf("reopen: %v", err)
				}
				b.StopTimer()
				_ = reopened.Close()
				b.StartTimer()
			}
		})
	}
}
