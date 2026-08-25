package store

import (
	"fmt"
	"testing"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// The state machine runs on the consensus goroutine, so every microsecond it
// spends applying is a microsecond the node is not replicating. These measure
// the apply path, the read path, and the two operations that touch the whole
// keyspace at once — snapshot and restore — which are the ones that decide how
// long compaction stalls a node.

func benchStore(b *testing.B, keys int) *Store {
	b.Helper()
	s := New()
	for i := 0; i < keys; i++ {
		cmd := &kvv1.Command{
			Op:    kvv1.OpType_OP_TYPE_PUT,
			Key:   []byte(fmt.Sprintf("key-%08d", i)),
			Value: []byte(fmt.Sprintf("value-%08d", i)),
		}
		data, err := EncodeCommand(cmd)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		if _, err := s.Apply(raft.Entry{Term: 1, Index: raft.Index(i + 1), Type: raft.EntryNormal, Data: data}); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
	return s
}

// BenchmarkApplyPut is the hot path: decode a command off the log and mutate the
// map. Every committed write in the cluster passes through it exactly once per
// node.
func BenchmarkApplyPut(b *testing.B) {
	s := New()
	data, err := EncodeCommand(&kvv1.Command{
		Op:    kvv1.OpType_OP_TYPE_PUT,
		Key:   []byte("key-00000042"),
		Value: []byte("a value of reasonable length"),
	})
	if err != nil {
		b.Fatalf("encode: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Apply(raft.Entry{Term: 1, Index: raft.Index(i + 1), Type: raft.EntryNormal, Data: data}); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
}

// BenchmarkApplyDeduplicated measures a retried command that the session table
// recognises. A client that times out and retries must not apply twice, and the
// check that prevents it runs on every write.
func BenchmarkApplyDeduplicated(b *testing.B) {
	s := New()
	data, err := EncodeCommand(&kvv1.Command{
		Op:     kvv1.OpType_OP_TYPE_PUT,
		Key:    []byte("k"),
		Value:  []byte("v"),
		Header: &kvv1.RequestHeader{ClientId: 7, Sequence: 1},
	})
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	if _, err := s.Apply(raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: data}); err != nil {
		b.Fatalf("seed apply: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Apply(raft.Entry{Term: 1, Index: raft.Index(i + 2), Type: raft.EntryNormal, Data: data}); err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
}

// BenchmarkGet is what a stale read costs once leadership has been established:
// nothing but a map lookup, which is the entire argument for offering the
// weaker read mode at all.
func BenchmarkGet(b *testing.B) {
	s := benchStore(b, 10_000)
	key := []byte("key-00005000")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Get(key); !ok {
			b.Fatal("key missing")
		}
	}
}

// BenchmarkSnapshot measures compaction's stall. It serialises the whole
// keyspace on the consensus goroutine, so its cost is downtime for replication,
// and it is the reason the snapshot threshold is a tuning knob.
func BenchmarkSnapshot(b *testing.B) {
	for _, keys := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			s := benchStore(b, keys)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := s.Snapshot()
				if err != nil {
					b.Fatalf("snapshot: %v", err)
				}
				b.SetBytes(int64(len(data)))
			}
		})
	}
}

// BenchmarkRestore is the other half: what a follower that fell behind the log
// boundary pays to catch up from a leader's snapshot.
func BenchmarkRestore(b *testing.B) {
	for _, keys := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			source := benchStore(b, keys)
			data, err := source.Snapshot()
			if err != nil {
				b.Fatalf("snapshot: %v", err)
			}

			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				target := New()
				if err := target.Restore(data, raft.Index(keys)); err != nil {
					b.Fatalf("restore: %v", err)
				}
			}
		})
	}
}
