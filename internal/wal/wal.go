// Package wal implements the crash-safe write-ahead log and snapshot store that
// give quorumkv its durability.
//
// The consensus core hands the driver a batch describing what must be durable
// before any message may be sent. This package is what makes that promise real:
// if it reports success, the data survives a crash, and if a crash happens
// mid-write, replay recovers exactly the prefix that was acknowledged and
// nothing more.
package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

const (
	// segmentSuffix names a log segment file.
	segmentSuffix = ".wal"
	// segmentPrefix precedes the first log index the segment contains, which
	// makes purging after compaction a filename comparison rather than a scan.
	segmentPrefix = "seg-"
	// defaultSegmentBytes rotates segments at 16 MiB. Segments exist so that
	// compaction can reclaim space by deleting whole files; the size is a
	// trade-off between reclaim granularity and the number of open files.
	defaultSegmentBytes = 16 << 20
)

// Options configures a Log.
type Options struct {
	// Dir is the directory holding segments and snapshots. It is created if
	// absent.
	Dir string

	// SegmentBytes is the size at which a new segment is started. Defaults to
	// 16 MiB.
	SegmentBytes int64

	// NoSync disables fsync. This is for tests and benchmarks only: with it set,
	// a crash can lose acknowledged writes, which breaks the guarantee the
	// consensus layer depends on. It is named for what it does rather than
	// something reassuring, precisely so that enabling it in production reads as
	// the mistake it would be.
	NoSync bool
}

func (o Options) withDefaults() Options {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = defaultSegmentBytes
	}
	return o
}

// Log is an append-only write-ahead log split into segments.
//
// Truncation deserves explanation, because the file format has no way to erase
// anything. When a follower discards a conflicting suffix and the leader's
// replacement entries are written, those entries carry indexes that have already
// appeared in the log. Replay resolves this by index with last-write-wins: a
// later record for index N supersedes any earlier one.
//
// That keeps the write path purely append-only, which matters for two reasons.
// Rewriting bytes in place is the operation most likely to leave a file
// half-updated after a crash, and an append-only file can be fsynced without any
// concern for the order in which earlier regions were modified.
type Log struct {
	opts Options

	// segments are the segment base indexes in ascending order.
	segments []raft.Index

	// active is the segment currently being appended to.
	active     *os.File
	activeBase raft.Index
	activeSize int64

	// lastIndex is the highest entry index written, used to name new segments.
	lastIndex raft.Index

	// dirtyBytes counts data written but not yet fsynced, purely for reporting.
	dirtyBytes int64

	// buf is reused across appends so that a steady write workload does not
	// allocate a fresh frame buffer per batch.
	buf []byte

	closed bool
}

// State is what replay recovers: everything needed to rebuild a node.
type State struct {
	// HardState is the last persisted term, vote and commit index.
	HardState raft.HardState
	// Entries are the recovered log entries, in index order, with conflicting
	// rewrites already resolved.
	Entries []raft.Entry
	// SnapshotIndex and SnapshotTerm mark the compaction boundary recorded in the
	// log, which tells the caller which snapshot file to load.
	SnapshotIndex raft.Index
	SnapshotTerm  raft.Term
	// TornAt is the offset of a torn tail record, if one was found. It is
	// informational: recovery already handled it by discarding the tail.
	TornAt int64
	// Torn reports whether a partial record was discarded during replay.
	Torn bool
}

// Open recovers and opens a log in the given directory, returning the state
// replay found.
//
// Recovery and opening are a single operation on purpose. A torn tail must be
// discarded before anything is appended, and separating the two would leave a
// window in which a caller could append after a partial record — producing a log
// that every future replay stops short of reading.
func Open(opts Options) (*Log, State, error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, State{}, errors.New("wal: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, State{}, fmt.Errorf("wal: create dir: %w", err)
	}

	st, err := Replay(opts.Dir)
	if err != nil {
		return nil, State{}, err
	}

	l := &Log{opts: opts}
	if err := l.loadSegments(); err != nil {
		return nil, State{}, err
	}
	// Continue numbering from what was recovered, so a new segment is named for
	// an index that actually follows the existing ones.
	l.lastIndex = st.SnapshotIndex
	if n := len(st.Entries); n > 0 {
		l.lastIndex = st.Entries[n-1].Index
	}
	return l, st, nil
}

func (l *Log) loadSegments() error {
	names, err := filepath.Glob(filepath.Join(l.opts.Dir, segmentPrefix+"*"+segmentSuffix))
	if err != nil {
		return fmt.Errorf("wal: list segments: %w", err)
	}
	l.segments = nil
	for _, name := range names {
		base, err := parseSegmentName(filepath.Base(name))
		if err != nil {
			// An unrecognised file in the data directory is not this package's to
			// interpret, and silently ignoring it could mean silently ignoring a
			// segment. Fail loudly instead.
			return fmt.Errorf("wal: unrecognised file %q in log directory: %w", filepath.Base(name), err)
		}
		l.segments = append(l.segments, base)
	}
	sort.Slice(l.segments, func(i, j int) bool { return l.segments[i] < l.segments[j] })
	return nil
}

func segmentName(base raft.Index) string {
	// Zero-padded so that lexical order matches numeric order, which keeps a
	// directory listing readable and makes glob results already sorted.
	return fmt.Sprintf("%s%020d%s", segmentPrefix, uint64(base), segmentSuffix)
}

func parseSegmentName(name string) (raft.Index, error) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, fmt.Errorf("not a segment name")
	}
	digits := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	v, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad segment index %q", digits)
	}
	return raft.Index(v), nil
}

func (l *Log) segmentPath(base raft.Index) string {
	return filepath.Join(l.opts.Dir, segmentName(base))
}

// openActive ensures there is a segment to append to.
func (l *Log) openActive(base raft.Index) error {
	if l.active != nil {
		return nil
	}
	if len(l.segments) > 0 {
		base = l.segments[len(l.segments)-1]
	} else {
		l.segments = append(l.segments, base)
	}
	f, err := os.OpenFile(l.segmentPath(base), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: stat segment: %w", err)
	}
	l.active = f
	l.activeBase = base
	l.activeSize = info.Size()
	// A newly created segment makes the directory entry itself something that
	// must survive a crash. Without this, the data could be durable while the
	// name pointing at it is not.
	return l.syncDir()
}

func (l *Log) syncDir() error {
	if l.opts.NoSync {
		return nil
	}
	d, err := os.Open(l.opts.Dir)
	if err != nil {
		return fmt.Errorf("wal: open dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync dir: %w", err)
	}
	return nil
}

// rotateIfNeeded starts a new segment once the active one is large enough.
func (l *Log) rotateIfNeeded(nextIndex raft.Index) error {
	if l.active == nil || l.activeSize < l.opts.SegmentBytes {
		return nil
	}
	// The active segment must be durable before it is closed: after rotation
	// nothing will write to it again, so there is no later fsync to cover it.
	if err := l.sync(); err != nil {
		return err
	}
	if err := l.active.Close(); err != nil {
		return fmt.Errorf("wal: close rotated segment: %w", err)
	}
	l.active = nil
	l.segments = append(l.segments, nextIndex)
	l.activeBase = nextIndex
	l.activeSize = 0

	f, err := os.OpenFile(l.segmentPath(nextIndex), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open new segment: %w", err)
	}
	l.active = f
	return l.syncDir()
}

// Append writes entries and, when non-empty, a hard state record.
//
// It does not fsync. Callers batch a whole Ready worth of writes and then call
// Sync exactly once, which is the difference between one disk barrier per commit
// round and one per entry.
func (l *Log) Append(entries []raft.Entry, hs raft.HardState) error {
	if l.closed {
		return errors.New("wal: log is closed")
	}
	if len(entries) == 0 && hs.IsEmpty() {
		return nil
	}

	base := raft.Index(1)
	if len(entries) > 0 {
		base = entries[0].Index
	} else if l.lastIndex > 0 {
		base = l.lastIndex + 1
	}
	if err := l.openActive(base); err != nil {
		return err
	}
	if len(entries) > 0 {
		if err := l.rotateIfNeeded(entries[0].Index); err != nil {
			return err
		}
	}

	l.buf = l.buf[:0]
	// The hard state is framed before the entries it describes. Order within a
	// single unsynced batch does not affect correctness, but writing the term and
	// vote first means that a tear in the middle of a batch loses entries rather
	// than the vote that authorised them.
	if !hs.IsEmpty() {
		l.buf = encodeRecord(l.buf, recordHardState, encodeHardState(hs))
	}
	for _, e := range entries {
		l.buf = encodeRecord(l.buf, recordEntry, encodeEntry(e))
	}

	if _, err := l.active.Write(l.buf); err != nil {
		return fmt.Errorf("wal: write records: %w", err)
	}
	l.activeSize += int64(len(l.buf))
	l.dirtyBytes += int64(len(l.buf))
	if len(entries) > 0 {
		if last := entries[len(entries)-1].Index; last > l.lastIndex {
			l.lastIndex = last
		}
	}
	return nil
}

// MarkSnapshot records a compaction boundary in the log, so that replay knows
// which snapshot to load and where the retained log begins.
func (l *Log) MarkSnapshot(index raft.Index, term raft.Term) error {
	if l.closed {
		return errors.New("wal: log is closed")
	}
	if err := l.openActive(index + 1); err != nil {
		return err
	}
	l.buf = l.buf[:0]
	l.buf = encodeRecord(l.buf, recordSnapshotMark, encodeSnapshotMark(index, term))
	if _, err := l.active.Write(l.buf); err != nil {
		return fmt.Errorf("wal: write snapshot mark: %w", err)
	}
	l.activeSize += int64(len(l.buf))
	if index > l.lastIndex {
		l.lastIndex = index
	}
	// A snapshot boundary is only useful if it outlives the crash that follows,
	// so this one barrier is not batched with anything.
	return l.Sync()
}

// Sync flushes buffered writes to stable storage. Until it returns, nothing
// written since the last call may be treated as durable.
func (l *Log) Sync() error {
	if l.closed {
		return errors.New("wal: log is closed")
	}
	return l.sync()
}

func (l *Log) sync() error {
	if l.active == nil || l.opts.NoSync {
		l.dirtyBytes = 0
		return nil
	}
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	l.dirtyBytes = 0
	return nil
}

// DirtyBytes reports how much has been written but not yet synced.
func (l *Log) DirtyBytes() int64 { return l.dirtyBytes }

// LastIndex reports the highest entry index written.
func (l *Log) LastIndex() raft.Index { return l.lastIndex }

// Segments reports the number of segment files, which is what compaction
// reclaims.
func (l *Log) Segments() int { return len(l.segments) }

// Close syncs and releases the log.
func (l *Log) Close() error {
	if l.closed {
		return nil
	}
	var err error
	if l.active != nil {
		err = l.sync()
		if cerr := l.active.Close(); err == nil {
			err = cerr
		}
		l.active = nil
	}
	l.closed = true
	return err
}

// Replay reconstructs state from every segment on disk.
//
// Entries are resolved by index with last-write-wins, so a suffix that was
// truncated and rewritten reads back as the rewrite. A torn tail record ends
// replay without error: it can only be the final record, and because the write
// was never acknowledged, nothing above the log was told it was durable.
func Replay(dir string) (State, error) {
	var st State

	names, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"+segmentSuffix))
	if err != nil {
		return st, fmt.Errorf("wal: list segments: %w", err)
	}
	if len(names) == 0 {
		return st, nil
	}
	sort.Strings(names)

	// byIndex resolves rewritten indexes; order records the first appearance of
	// each index so the result is stable and does not depend on map iteration.
	byIndex := make(map[raft.Index]raft.Entry)
	var order []raft.Index

	for si, name := range names {
		data, err := os.ReadFile(name)
		if err != nil {
			return st, fmt.Errorf("wal: read %s: %w", filepath.Base(name), err)
		}

		var offset int64
		for offset < int64(len(data)) {
			t, payload, size, err := decodeRecord(data[offset:], offset)
			if err != nil {
				var torn *errTornRecord
				if !errors.As(err, &torn) {
					return st, err
				}
				// A tear is only acceptable at the very end of the log. Finding one
				// in an earlier segment means a completed segment was damaged, which
				// is real corruption rather than an interrupted write.
				if si != len(names)-1 {
					return st, fmt.Errorf("wal: corruption in completed segment %s: %w",
						filepath.Base(name), err)
				}
				st.Torn = true
				st.TornAt = offset
				// Discard the partial tail so the next append starts from a clean
				// boundary. Leaving it in place would make every future replay
				// stop here, silently capping the log.
				if err := truncateFile(name, offset); err != nil {
					return st, err
				}
				offset = int64(len(data))
				break
			}

			switch t {
			case recordEntry:
				e, err := decodeEntry(payload)
				if err != nil {
					return st, err
				}
				if _, seen := byIndex[e.Index]; !seen {
					order = append(order, e.Index)
				}
				byIndex[e.Index] = e
			case recordHardState:
				hs, err := decodeHardState(payload)
				if err != nil {
					return st, err
				}
				st.HardState = hs
			case recordSnapshotMark:
				idx, term, err := decodeSnapshotMark(payload)
				if err != nil {
					return st, err
				}
				if idx >= st.SnapshotIndex {
					st.SnapshotIndex, st.SnapshotTerm = idx, term
				}
			}
			offset += int64(size)
		}
	}

	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	for _, idx := range order {
		// Entries at or below the compaction boundary are already captured by the
		// snapshot; replaying them would double-apply them to the state machine.
		if idx <= st.SnapshotIndex {
			continue
		}
		st.Entries = append(st.Entries, byIndex[idx])
	}

	// The recovered log must be contiguous. A gap would mean an entry was lost
	// while its successors survived, which no amount of later replication can
	// repair safely, so it is reported rather than papered over.
	for i := 1; i < len(st.Entries); i++ {
		if st.Entries[i].Index != st.Entries[i-1].Index+1 {
			return st, fmt.Errorf("wal: gap in recovered log between index %d and %d",
				st.Entries[i-1].Index, st.Entries[i].Index)
		}
	}
	if len(st.Entries) > 0 && st.SnapshotIndex > 0 {
		if want := st.SnapshotIndex + 1; st.Entries[0].Index != want {
			return st, fmt.Errorf("wal: recovered log starts at index %d, expected %d after the snapshot boundary",
				st.Entries[0].Index, want)
		}
	}
	return st, nil
}

func truncateFile(name string, size int64) error {
	f, err := os.OpenFile(name, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open for truncate: %w", err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("wal: truncate %s to %d: %w", filepath.Base(name), size, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate: %w", err)
	}
	return nil
}

// PurgeUpTo deletes segments made redundant by a snapshot through index, and
// reports how many were removed.
//
// A segment may only go once every entry in it is covered by the snapshot, which
// is decided by the *next* segment's base index: if that is at or below index+1,
// this segment holds nothing newer. The active segment is never purged, since it
// may still be receiving writes.
func (l *Log) PurgeUpTo(index raft.Index) (int, error) {
	if l.closed {
		return 0, errors.New("wal: log is closed")
	}
	removed := 0
	for i := 0; i+1 < len(l.segments); i++ {
		nextBase := l.segments[i+1]
		if nextBase > index+1 {
			break
		}
		if l.segments[i] == l.activeBase {
			break
		}
		if err := os.Remove(l.segmentPath(l.segments[i])); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("wal: purge segment: %w", err)
		}
		removed++
	}
	if removed > 0 {
		l.segments = l.segments[removed:]
		// The deletions themselves must be durable, or a crash could resurrect a
		// segment holding entries the snapshot has superseded.
		if err := l.syncDir(); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// verifyReadable is a helper for tests and tooling: it walks every record and
// reports the first problem found, without changing anything on disk.
func verifyReadable(dir string) error {
	names, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"+segmentSuffix))
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		var offset int64
		for offset < int64(len(data)) {
			_, _, size, err := decodeRecord(data[offset:], offset)
			if err != nil {
				return fmt.Errorf("%s: %w", filepath.Base(name), err)
			}
			offset += int64(size)
		}
	}
	return nil
}
