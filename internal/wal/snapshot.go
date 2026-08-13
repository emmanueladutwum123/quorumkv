package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

const (
	snapshotPrefix = "snap-"
	snapshotSuffix = ".snap"
	// tmpSuffix marks a snapshot still being written. A crash leaves such a file
	// behind, and because it never had its final name it can never be mistaken
	// for a complete snapshot.
	tmpSuffix = ".tmp"
	// defaultSnapshotsKept retains a few generations rather than only the newest.
	// A snapshot is the only copy of the compacted prefix, so if the newest one
	// turns out to be unreadable, having an older one is the difference between a
	// slower recovery and no recovery at all.
	defaultSnapshotsKept = 3
)

// ErrNoSnapshot means the directory holds no readable snapshot.
var ErrNoSnapshot = errors.New("wal: no snapshot found")

// SnapshotStore persists state machine snapshots.
//
// Installation is atomic by construction: a snapshot is written to a temporary
// name, fsynced, and only then renamed into place. Rename is atomic on POSIX
// filesystems, so a reader either sees the previous snapshot or the complete new
// one, never a partial file. Writing in place instead would leave a window where
// a crash destroys the only copy of the compacted prefix.
type SnapshotStore struct {
	dir    string
	keep   int
	noSync bool
}

// NewSnapshotStore opens a snapshot store in dir.
func NewSnapshotStore(dir string, opts ...SnapshotOption) (*SnapshotStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create snapshot dir: %w", err)
	}
	s := &SnapshotStore{dir: dir, keep: defaultSnapshotsKept}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// SnapshotOption configures a SnapshotStore.
type SnapshotOption func(*SnapshotStore)

// WithKeep sets how many snapshot generations to retain.
func WithKeep(n int) SnapshotOption {
	return func(s *SnapshotStore) {
		if n > 0 {
			s.keep = n
		}
	}
}

// WithNoSync disables fsync, for tests and benchmarks only.
func WithNoSync() SnapshotOption {
	return func(s *SnapshotStore) { s.noSync = true }
}

func snapshotName(index raft.Index, term raft.Term) string {
	return fmt.Sprintf("%s%020d-%020d%s", snapshotPrefix, uint64(index), uint64(term), snapshotSuffix)
}

func parseSnapshotName(name string) (raft.Index, raft.Term, error) {
	if !strings.HasPrefix(name, snapshotPrefix) || !strings.HasSuffix(name, snapshotSuffix) {
		return 0, 0, fmt.Errorf("not a snapshot name")
	}
	body := name[len(snapshotPrefix) : len(name)-len(snapshotSuffix)]
	parts := strings.Split(body, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed snapshot name %q", name)
	}
	idx, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad snapshot index in %q", name)
	}
	term, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad snapshot term in %q", name)
	}
	return raft.Index(idx), raft.Term(term), nil
}

// Save writes a snapshot atomically and purges older generations beyond the
// retention limit.
func (s *SnapshotStore) Save(snap raft.Snapshot) error {
	payload, err := encodeSnapshot(snap)
	if err != nil {
		return err
	}
	framed := encodeRecord(nil, recordEntry, payload)

	final := filepath.Join(s.dir, snapshotName(snap.Index, snap.Term))
	tmp := final + tmpSuffix

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create snapshot: %w", err)
	}
	if _, err := f.Write(framed); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("wal: write snapshot: %w", err)
	}
	// The contents must reach stable storage before the rename, otherwise the
	// name could become visible while pointing at data that is not there yet.
	if !s.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("wal: sync snapshot: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("wal: close snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("wal: install snapshot: %w", err)
	}
	// And the rename itself must be durable, or a crash could leave neither name.
	if err := s.syncDir(); err != nil {
		return err
	}
	return s.purge()
}

func (s *SnapshotStore) syncDir() error {
	if s.noSync {
		return nil
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("wal: open snapshot dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync snapshot dir: %w", err)
	}
	return nil
}

// generations lists stored snapshots, newest first.
func (s *SnapshotStore) generations() ([]raft.Index, map[raft.Index]string, error) {
	names, err := filepath.Glob(filepath.Join(s.dir, snapshotPrefix+"*"+snapshotSuffix))
	if err != nil {
		return nil, nil, fmt.Errorf("wal: list snapshots: %w", err)
	}
	paths := make(map[raft.Index]string, len(names))
	var idxs []raft.Index
	for _, name := range names {
		idx, _, err := parseSnapshotName(filepath.Base(name))
		if err != nil {
			continue
		}
		// With several snapshots at one index, the lexically greatest name wins,
		// which is the highest term — the most recent of them.
		if prev, ok := paths[idx]; !ok || name > prev {
			if !ok {
				idxs = append(idxs, idx)
			}
			paths[idx] = name
		}
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] > idxs[j] })
	return idxs, paths, nil
}

// Latest returns the newest readable snapshot.
//
// If the newest file fails its checksum it is skipped in favour of an older
// generation, because a slower recovery from a valid older snapshot is strictly
// better than refusing to start.
func (s *SnapshotStore) Latest() (raft.Snapshot, error) {
	idxs, paths, err := s.generations()
	if err != nil {
		return raft.Snapshot{}, err
	}
	var firstErr error
	for _, idx := range idxs {
		snap, err := loadSnapshot(paths[idx])
		if err == nil {
			return snap, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return raft.Snapshot{}, fmt.Errorf("wal: no readable snapshot (newest failed: %w)", firstErr)
	}
	return raft.Snapshot{}, ErrNoSnapshot
}

// Count reports how many snapshot generations are stored.
func (s *SnapshotStore) Count() int {
	idxs, _, err := s.generations()
	if err != nil {
		return 0
	}
	return len(idxs)
}

// purge removes generations beyond the retention limit, oldest first.
func (s *SnapshotStore) purge() error {
	idxs, paths, err := s.generations()
	if err != nil {
		return err
	}
	if len(idxs) <= s.keep {
		return nil
	}
	for _, idx := range idxs[s.keep:] {
		if err := os.Remove(paths[idx]); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: purge snapshot: %w", err)
		}
	}
	return s.syncDir()
}

func loadSnapshot(path string) (raft.Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("wal: read snapshot: %w", err)
	}
	_, payload, _, err := decodeRecord(data, 0)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("wal: snapshot %s: %w", filepath.Base(path), err)
	}
	return decodeSnapshot(payload)
}

// --- snapshot payload encoding --------------------------------------------

func encodeSnapshot(snap raft.Snapshot) ([]byte, error) {
	if err := snap.Conf.Validate(); err != nil {
		// A snapshot without a valid configuration is unusable: a node restoring
		// from it would have no idea who its peers are.
		return nil, fmt.Errorf("wal: snapshot configuration invalid: %w", err)
	}
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(snap.Index))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(snap.Term))
	buf = appendNodeSet(buf, snap.Conf.Voters[0])
	buf = appendNodeSet(buf, snap.Conf.Voters[1])
	buf = appendNodeSet(buf, snap.Conf.Learners)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(snap.Data)))
	return append(buf, snap.Data...), nil
}

func appendNodeSet(buf []byte, s raft.NodeSet) []byte {
	ids := s.Sorted() // sorted, so the encoding of a given set is byte-stable
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ids)))
	for _, id := range ids {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(id))
	}
	return buf
}

func decodeSnapshot(payload []byte) (raft.Snapshot, error) {
	r := &reader{buf: payload}
	var snap raft.Snapshot
	snap.Index = raft.Index(r.uint64())
	snap.Term = raft.Term(r.uint64())

	var err error
	if snap.Conf.Voters[0], err = readNodeSet(r); err != nil {
		return snap, err
	}
	if snap.Conf.Voters[1], err = readNodeSet(r); err != nil {
		return snap, err
	}
	if snap.Conf.Learners, err = readNodeSet(r); err != nil {
		return snap, err
	}

	dataLen := r.uint64()
	if r.err != nil {
		return snap, r.err
	}
	if uint64(len(payload)-r.off) != dataLen {
		return snap, fmt.Errorf("wal: snapshot data length %d does not match remaining %d bytes",
			dataLen, len(payload)-r.off)
	}
	if dataLen > 0 {
		snap.Data = make([]byte, dataLen)
		copy(snap.Data, payload[r.off:])
	}
	return snap, nil
}

func readNodeSet(r *reader) (raft.NodeSet, error) {
	n := r.uint32()
	if r.err != nil {
		return nil, r.err
	}
	if uint64(n)*8 > uint64(len(r.buf)-r.off) {
		return nil, fmt.Errorf("wal: snapshot node set claims %d members, beyond payload", n)
	}
	set := make(raft.NodeSet, n)
	for i := uint32(0); i < n; i++ {
		set[raft.NodeID(r.uint64())] = struct{}{}
	}
	return set, r.err
}

// reader is a bounds-checked cursor over a payload, so that a malformed length
// produces an error rather than a panic.
type reader struct {
	buf []byte
	off int
	err error
}

func (r *reader) uint32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.off+4 > len(r.buf) {
		r.err = fmt.Errorf("wal: snapshot payload truncated at offset %d", r.off)
		return 0
	}
	v := binary.LittleEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v
}

func (r *reader) uint64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.off+8 > len(r.buf) {
		r.err = fmt.Errorf("wal: snapshot payload truncated at offset %d", r.off)
		return 0
	}
	v := binary.LittleEndian.Uint64(r.buf[r.off:])
	r.off += 8
	return v
}
