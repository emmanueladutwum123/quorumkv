package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

func entries(base raft.Index, terms ...raft.Term) []raft.Entry {
	out := make([]raft.Entry, len(terms))
	for i, term := range terms {
		out[i] = raft.Entry{
			Term:  term,
			Index: base + raft.Index(i),
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf("value-%d", base+raft.Index(i))),
		}
	}
	return out
}

func openLog(t testing.TB, dir string) (*Log, State) {
	t.Helper()
	l, st, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, st
}

// segmentFiles returns the segment paths in order.
func segmentFiles(t testing.TB, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"+segmentSuffix))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return names
}

// --- round trips -----------------------------------------------------------

func TestAppendAndReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	want := entries(1, 1, 1, 2, 2, 3)
	hs := raft.HardState{Term: 3, Vote: 7, Commit: 4}
	if err := l.Append(want, hs); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if st.Torn {
		t.Error("replay reported a tear in a cleanly written log")
	}
	if st.HardState != hs {
		t.Errorf("hard state = %+v, want %+v", st.HardState, hs)
	}
	assertEntriesEqual(t, st.Entries, want)
}

func TestReplayOfEmptyDirectory(t *testing.T) {
	st, err := Replay(t.TempDir())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(st.Entries) != 0 || !st.HardState.IsEmpty() {
		t.Errorf("recovered %d entries and %+v from an empty directory", len(st.Entries), st.HardState)
	}
}

func TestHardStateLatestWins(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	for term := raft.Term(1); term <= 5; term++ {
		if err := l.Append(nil, raft.HardState{Term: term, Vote: raft.NodeID(term)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if st.HardState.Term != 5 || st.HardState.Vote != 5 {
		t.Errorf("hard state = %+v, want the most recent (term 5, vote 5)", st.HardState)
	}
}

func TestEmptyAppendIsNoOp(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	if err := l.Append(nil, raft.HardState{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := len(segmentFiles(t, dir)); got != 0 {
		t.Errorf("an empty append created %d segment files, want 0", got)
	}
}

func TestAppendAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	l, _ := openLog(t, dir)
	if err := l.Append(entries(1, 1, 1, 1), raft.HardState{Term: 1, Commit: 3}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopening must pick up where the previous run left off, so that the
	// recovered log stays contiguous across a restart.
	l2, st := openLog(t, dir)
	if got := st.Entries[len(st.Entries)-1].Index; got != 3 {
		t.Fatalf("recovered last index = %d, want 3", got)
	}
	if got := l2.LastIndex(); got != 3 {
		t.Errorf("LastIndex() = %d after reopen, want 3", got)
	}
	if err := l2.Append(entries(4, 2, 2), raft.HardState{Term: 2, Commit: 5}); err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if err := l2.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	final, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	assertEntriesEqual(t, final.Entries, append(entries(1, 1, 1, 1), entries(4, 2, 2)...))
}

// --- truncation is expressed as a rewrite ---------------------------------

func TestRewrittenIndexesSupersedeEarlierOnes(t *testing.T) {
	// A follower that discards a conflicting suffix writes the leader's
	// replacement entries at indexes already present in the file. The format
	// cannot erase, so replay must resolve by index with last-write-wins.
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	if err := l.Append(entries(1, 1, 1, 1, 1, 1), raft.HardState{Term: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// A new leader in term 4 replaces indexes 3 onward with two entries.
	replacement := entries(3, 4, 4)
	if err := l.Append(replacement, raft.HardState{Term: 4}); err != nil {
		t.Fatalf("Append replacement: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// Indexes 1-2 survive from the original write; 3-4 are the rewrite. Index 5
	// was written once at term 1 and never superseded, so it is still present —
	// the caller's log, not the WAL, decides where the log ends.
	if len(st.Entries) != 5 {
		t.Fatalf("recovered %d entries, want 5: %v", len(st.Entries), summarise(st.Entries))
	}
	wantTerms := []raft.Term{1, 1, 4, 4, 1}
	for i, want := range wantTerms {
		if got := st.Entries[i].Term; got != want {
			t.Errorf("index %d term = %d, want %d (recovered: %v)",
				i+1, got, want, summarise(st.Entries))
		}
	}
}

// --- torn tails ------------------------------------------------------------

func TestTornTailIsDiscardedAndPrefixSurvives(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	want := entries(1, 1, 1, 1, 1)
	if err := l.Append(want, raft.HardState{Term: 1, Commit: 5}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash partway through writing one more record.
	path := segmentFiles(t, dir)[0]
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	partial := append([]byte(nil), full...)
	partial = append(partial, 0x11, 0x22, 0x33) // a few bytes of a record header
	if err := os.WriteFile(path, partial, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay must recover from a torn tail, got: %v", err)
	}
	if !st.Torn {
		t.Error("replay did not report the tear")
	}
	if st.TornAt != int64(len(full)) {
		t.Errorf("TornAt = %d, want %d", st.TornAt, len(full))
	}
	assertEntriesEqual(t, st.Entries, want)

	// The tear must be removed from the file, or every future replay would stop
	// at the same place and silently cap the log.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read after replay: %v", err)
	}
	if len(after) != len(full) {
		t.Errorf("file is %d bytes after replay, want %d (the partial tail should be gone)",
			len(after), len(full))
	}
	second, err := Replay(dir)
	if err != nil {
		t.Fatalf("second Replay: %v", err)
	}
	if second.Torn {
		t.Error("the second replay still reports a tear; the truncation did not stick")
	}
}

func TestAppendWorksAfterTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	if err := l.Append(entries(1, 1, 1), raft.HardState{Term: 1, Commit: 2}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	path := segmentFiles(t, dir)[0]
	full, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(append([]byte(nil), full...), 0xff, 0xff), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Open recovers and truncates, so the next append lands on a clean boundary.
	l2, st, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l2.Close()
	if !st.Torn {
		t.Error("Open did not report the tear")
	}
	if err := l2.Append(entries(3, 2), raft.HardState{Term: 2, Commit: 3}); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if err := l2.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	final, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	assertEntriesEqual(t, final.Entries, append(entries(1, 1, 1), entries(3, 2)...))
	if err := verifyReadable(dir); err != nil {
		t.Errorf("log is not cleanly readable after recovery: %v", err)
	}
}

func TestTruncationAtEveryOffsetRecoversAValidPrefix(t *testing.T) {
	// A crash can interrupt a write at any byte. For every possible truncation
	// point, replay must succeed, never panic, and return a prefix of what was
	// written — never a corrupted or invented entry.
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	want := entries(1, 1, 1, 2, 2, 3, 3, 4)
	if err := l.Append(want, raft.HardState{Term: 4, Commit: 8}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	golden, err := os.ReadFile(segmentFiles(t, dir)[0])
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for cut := 0; cut <= len(golden); cut++ {
		sub := t.TempDir()
		path := filepath.Join(sub, segmentName(1))
		if err := os.WriteFile(path, golden[:cut], 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		st, err := Replay(sub)
		if err != nil {
			t.Fatalf("cut at %d: Replay failed: %v", cut, err)
		}
		if len(st.Entries) > len(want) {
			t.Fatalf("cut at %d: recovered %d entries, more than the %d written",
				cut, len(st.Entries), len(want))
		}
		// Whatever survived must match what was written, exactly and in order.
		for i, e := range st.Entries {
			if e.Index != want[i].Index || e.Term != want[i].Term || string(e.Data) != string(want[i].Data) {
				t.Fatalf("cut at %d: entry %d = (idx %d, term %d, %q), want (idx %d, term %d, %q)",
					cut, i, e.Index, e.Term, e.Data, want[i].Index, want[i].Term, want[i].Data)
			}
		}
	}
}

// --- corruption ------------------------------------------------------------

func TestChecksumDetectsBitFlip(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	if err := l.Append(entries(1, 1, 1, 1, 1), raft.HardState{Term: 1, Commit: 5}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	path := segmentFiles(t, dir)[0]
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip a bit inside the payload of an early record. The checksum must catch
	// it: silently returning altered data is the one outcome a log must never
	// produce.
	flipAt := headerSize + 4
	data[flipAt] ^= 0x80
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := verifyReadable(dir); err == nil {
		t.Fatal("a bit flip inside a record was not detected")
	} else if !strings.Contains(err.Error(), "checksum") {
		t.Errorf("error = %v, want it to name a checksum mismatch", err)
	}
}

func TestCorruptionInCompletedSegmentIsAnError(t *testing.T) {
	// A tear is only forgivable at the very end of the log, where it means an
	// interrupted write. Damage to an earlier, completed segment is real
	// corruption and must not be silently truncated away — doing so would
	// discard committed entries that follow it.
	dir := t.TempDir()
	l, _, err := Open(Options{Dir: dir, SegmentBytes: 256})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := raft.Index(1); i <= 40; i++ {
		if err := l.Append(entries(i, 1), raft.HardState{Term: 1, Commit: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Close()

	files := segmentFiles(t, dir)
	if len(files) < 2 {
		t.Fatalf("expected rotation to produce multiple segments, got %d", len(files))
	}

	// Damage the tail of the first (completed) segment.
	first := files[0]
	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := os.WriteFile(first, data[:len(data)-3], 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := Replay(dir); err == nil {
		t.Fatal("corruption in a completed segment was accepted")
	} else if !strings.Contains(err.Error(), "completed segment") {
		t.Errorf("error = %v, want it to identify a completed segment", err)
	}
}

func TestImplausibleLengthIsRejected(t *testing.T) {
	// A corrupted length field must be refused before any allocation, or a
	// damaged byte becomes an out-of-memory crash.
	dir := t.TempDir()
	path := filepath.Join(dir, segmentName(1))
	buf := make([]byte, headerSize)
	// A length far above maxRecordSize.
	buf[0], buf[1], buf[2], buf[3] = 0xff, 0xff, 0xff, 0x7f
	buf[4] = byte(recordEntry)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !st.Torn {
		t.Error("an implausible length was not treated as a tear")
	}
	if len(st.Entries) != 0 {
		t.Errorf("recovered %d entries from a single corrupt record", len(st.Entries))
	}
}

func TestUnknownFileInLogDirectoryIsRejected(t *testing.T) {
	// Silently ignoring an unrecognised file would mean silently ignoring a
	// segment whose name this build does not understand.
	dir := t.TempDir()
	bad := filepath.Join(dir, segmentPrefix+"not-a-number"+segmentSuffix)
	if err := os.WriteFile(bad, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := Open(Options{Dir: dir}); err == nil {
		t.Error("Open accepted an unrecognised file in the log directory")
	}
}

func TestGapInRecoveredLogIsRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	if err := l.Append(entries(1, 1, 1), raft.HardState{Term: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Index 5 with 3 and 4 missing. No later replication can repair a hole in the
	// middle of a log safely, so it must be reported rather than papered over.
	if err := l.Append(entries(5, 1), raft.HardState{Term: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Sync()

	if _, err := Replay(dir); err == nil {
		t.Error("a gap in the recovered log was accepted")
	} else if !strings.Contains(err.Error(), "gap") {
		t.Errorf("error = %v, want it to name the gap", err)
	}
}

// --- segments and compaction ----------------------------------------------

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(Options{Dir: dir, SegmentBytes: 512})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	for i := raft.Index(1); i <= 60; i++ {
		if err := l.Append(entries(i, 1), raft.HardState{Term: 1, Commit: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Sync()

	if got := l.Segments(); got < 2 {
		t.Errorf("Segments() = %d, want rotation to have produced at least 2", got)
	}
	// Rotation must not affect what can be read back.
	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(st.Entries) != 60 {
		t.Errorf("recovered %d entries across segments, want 60", len(st.Entries))
	}
}

func TestPurgeUpToReclaimsCoveredSegments(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(Options{Dir: dir, SegmentBytes: 256})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	for i := raft.Index(1); i <= 80; i++ {
		if err := l.Append(entries(i, 1), raft.HardState{Term: 1, Commit: i}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Sync()
	before := l.Segments()
	if before < 3 {
		t.Fatalf("expected several segments, got %d", before)
	}

	// A snapshot through index 40 makes every segment entirely below it
	// redundant.
	removed, err := l.PurgeUpTo(40)
	if err != nil {
		t.Fatalf("PurgeUpTo: %v", err)
	}
	if removed == 0 {
		t.Fatal("PurgeUpTo removed nothing despite a snapshot covering half the log")
	}
	if got := l.Segments(); got != before-removed {
		t.Errorf("Segments() = %d, want %d", got, before-removed)
	}

	// Everything above the snapshot boundary must still be readable.
	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay after purge: %v", err)
	}
	if len(st.Entries) == 0 {
		t.Fatal("purge removed every entry")
	}
	last := st.Entries[len(st.Entries)-1].Index
	if last != 80 {
		t.Errorf("last recovered index = %d, want 80", last)
	}
}

func TestPurgeKeepsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)
	if err := l.Append(entries(1, 1, 1, 1), raft.HardState{Term: 1, Commit: 3}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Sync()

	// Even a boundary above everything written must not remove the segment still
	// being appended to.
	removed, err := l.PurgeUpTo(100)
	if err != nil {
		t.Fatalf("PurgeUpTo: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed %d segments, want 0: the active segment must survive", removed)
	}
}

func TestMarkSnapshotSkipsCoveredEntriesOnReplay(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	if err := l.Append(entries(1, 1, 1, 1, 1, 1), raft.HardState{Term: 1, Commit: 5}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.MarkSnapshot(3, 1); err != nil {
		t.Fatalf("MarkSnapshot: %v", err)
	}
	l.Sync()

	st, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if st.SnapshotIndex != 3 || st.SnapshotTerm != 1 {
		t.Errorf("snapshot boundary = (%d, %d), want (3, 1)", st.SnapshotIndex, st.SnapshotTerm)
	}
	// Entries covered by the snapshot must not be replayed, or the state machine
	// would apply them a second time.
	if len(st.Entries) != 2 {
		t.Fatalf("recovered %d entries, want 2 (indexes 4 and 5): %v",
			len(st.Entries), summarise(st.Entries))
	}
	if st.Entries[0].Index != 4 {
		t.Errorf("first recovered index = %d, want 4", st.Entries[0].Index)
	}
}

// --- durability accounting -------------------------------------------------

func TestSyncClearsDirtyBytes(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(t, dir)

	if err := l.Append(entries(1, 1, 1, 1), raft.HardState{Term: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if l.DirtyBytes() == 0 {
		t.Error("DirtyBytes() = 0 after an append with no sync")
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if l.DirtyBytes() != 0 {
		t.Errorf("DirtyBytes() = %d after Sync", l.DirtyBytes())
	}
}

func TestOperationsAfterCloseFail(t *testing.T) {
	dir := t.TempDir()
	l, _, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Append(entries(1, 1), raft.HardState{Term: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Append(entries(2, 1), raft.HardState{}); err == nil {
		t.Error("Append succeeded after Close")
	}
	if err := l.Sync(); err == nil {
		t.Error("Sync succeeded after Close")
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close returned %v, want nil (Close should be idempotent)", err)
	}
}

func TestOpenRequiresDir(t *testing.T) {
	if _, _, err := Open(Options{}); err == nil {
		t.Error("Open succeeded without a directory")
	}
}

// --- record encoding -------------------------------------------------------

func TestRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		typ     recordType
		payload []byte
	}{
		{"entry", recordEntry, encodeEntry(raft.Entry{Term: 3, Index: 9, Type: raft.EntryNormal, Data: []byte("hello")})},
		{"entry with no data", recordEntry, encodeEntry(raft.Entry{Term: 1, Index: 1, Type: raft.EntryNoOp})},
		{"hard state", recordHardState, encodeHardState(raft.HardState{Term: 4, Vote: 2, Commit: 8})},
		{"snapshot mark", recordSnapshotMark, encodeSnapshotMark(12, 5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			framed := encodeRecord(nil, tc.typ, tc.payload)
			gotType, gotPayload, size, err := decodeRecord(framed, 0)
			if err != nil {
				t.Fatalf("decodeRecord: %v", err)
			}
			if gotType != tc.typ {
				t.Errorf("type = %v, want %v", gotType, tc.typ)
			}
			if size != len(framed) {
				t.Errorf("size = %d, want %d", size, len(framed))
			}
			if string(gotPayload) != string(tc.payload) {
				t.Error("payload did not survive the round trip")
			}
		})
	}
}

func TestEntryRoundTripPreservesData(t *testing.T) {
	want := raft.Entry{Term: 7, Index: 42, Type: raft.EntryConfChange, Data: []byte{0x00, 0xff, 0x7f, 0x80}}
	got, err := decodeEntry(encodeEntry(want))
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if got.Term != want.Term || got.Index != want.Index || got.Type != want.Type {
		t.Errorf("header = %+v, want %+v", got, want)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("data = %v, want %v", got.Data, want.Data)
	}
}

func TestDecodeEntryDoesNotAliasBuffer(t *testing.T) {
	// The decoder is handed a reusable read buffer, so an entry that aliased it
	// would change out from under the caller on the next record.
	buf := encodeEntry(raft.Entry{Term: 1, Index: 1, Data: []byte("original")})
	e, err := decodeEntry(buf)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	for i := range buf {
		buf[i] = 0
	}
	if string(e.Data) != "original" {
		t.Errorf("data = %q after the source buffer was overwritten, want %q", e.Data, "original")
	}
}

func TestDecodeRejectsShortPayloads(t *testing.T) {
	if _, err := decodeEntry([]byte{1, 2, 3}); err == nil {
		t.Error("decodeEntry accepted a payload shorter than its header")
	}
	if _, err := decodeHardState([]byte{1, 2, 3}); err == nil {
		t.Error("decodeHardState accepted a short payload")
	}
	if _, _, err := decodeSnapshotMark([]byte{1, 2, 3}); err == nil {
		t.Error("decodeSnapshotMark accepted a short payload")
	}
	var torn *errTornRecord
	if _, _, _, err := decodeRecord([]byte{1, 2}, 0); !errors.As(err, &torn) {
		t.Errorf("decodeRecord error = %v, want a torn-record error", err)
	}
}

// --- helpers ---------------------------------------------------------------

func assertEntriesEqual(t testing.TB, got, want []raft.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("recovered %d entries, want %d\n got: %v\nwant: %v",
			len(got), len(want), summarise(got), summarise(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term ||
			got[i].Type != want[i].Type || string(got[i].Data) != string(want[i].Data) {
			t.Errorf("entry %d = (idx %d, term %d, %q), want (idx %d, term %d, %q)",
				i, got[i].Index, got[i].Term, got[i].Data,
				want[i].Index, want[i].Term, want[i].Data)
		}
	}
}

func summarise(ents []raft.Entry) string {
	parts := make([]string, len(ents))
	for i, e := range ents {
		parts[i] = fmt.Sprintf("%d@%d", e.Index, e.Term)
	}
	return "[" + strings.Join(parts, " ") + "]"
}
