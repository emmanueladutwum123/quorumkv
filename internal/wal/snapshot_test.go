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

func testSnapshot(index raft.Index, term raft.Term, data string) raft.Snapshot {
	return raft.Snapshot{
		Index: index,
		Term:  term,
		Conf:  raft.NewConfig(1, 2, 3),
		Data:  []byte(data),
	}
}

func snapshotFiles(t testing.TB, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, snapshotPrefix+"*"+snapshotSuffix))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return names
}

func TestSnapshotSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSnapshotStore(dir)
	if err != nil {
		t.Fatalf("NewSnapshotStore: %v", err)
	}

	want := testSnapshot(42, 7, "state-machine-contents")
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Index != want.Index || got.Term != want.Term {
		t.Errorf("boundary = (%d, %d), want (%d, %d)", got.Index, got.Term, want.Index, want.Term)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("data = %q, want %q", got.Data, want.Data)
	}
}

func TestSnapshotPreservesMembership(t *testing.T) {
	// A restoring node has no log left to replay membership changes from, so the
	// configuration must survive inside the snapshot itself.
	dir := t.TempDir()
	s, err := NewSnapshotStore(dir)
	if err != nil {
		t.Fatalf("NewSnapshotStore: %v", err)
	}

	want := raft.Snapshot{
		Index: 10,
		Term:  3,
		Conf: raft.Config{
			Voters:   [2]raft.NodeSet{raft.NewNodeSet(1, 2, 3), raft.NewNodeSet(4, 5)},
			Learners: raft.NewNodeSet(9),
		},
		Data: []byte("x"),
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	if !got.Conf.IsJoint() {
		t.Error("the joint configuration was lost")
	}
	for _, id := range []raft.NodeID{1, 2, 3} {
		if !got.Conf.Voters[0].Contains(id) {
			t.Errorf("incoming voter %d missing", id)
		}
	}
	for _, id := range []raft.NodeID{4, 5} {
		if !got.Conf.Voters[1].Contains(id) {
			t.Errorf("outgoing voter %d missing", id)
		}
	}
	if !got.Conf.IsLearner(9) {
		t.Error("learner 9 missing")
	}
}

func TestSnapshotWithEmptyDataRoundTrips(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)
	if err := s.Save(raft.Snapshot{Index: 1, Term: 1, Conf: raft.NewConfig(1)}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Index != 1 || len(got.Data) != 0 {
		t.Errorf("got index %d with %d bytes of data, want index 1 with 0", got.Index, len(got.Data))
	}
}

func TestSnapshotRejectsInvalidConfiguration(t *testing.T) {
	// A snapshot carrying no voters would leave a restoring node unable to
	// participate, and unable to tell that anything was wrong.
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)
	if err := s.Save(raft.Snapshot{Index: 1, Term: 1}); err == nil {
		t.Error("Save accepted a snapshot with no configuration")
	}
}

func TestLatestPrefersHighestIndex(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)

	for _, idx := range []raft.Index{5, 20, 12} {
		if err := s.Save(testSnapshot(idx, 1, fmt.Sprintf("at-%d", idx))); err != nil {
			t.Fatalf("Save(%d): %v", idx, err)
		}
	}
	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Index != 20 {
		t.Errorf("Latest() index = %d, want 20", got.Index)
	}
}

func TestNoSnapshotReported(t *testing.T) {
	s, _ := NewSnapshotStore(t.TempDir())
	if _, err := s.Latest(); !errors.Is(err, ErrNoSnapshot) {
		t.Errorf("Latest() error = %v, want ErrNoSnapshot", err)
	}
}

func TestCorruptNewestSnapshotFallsBackToOlder(t *testing.T) {
	// A snapshot is the only copy of the compacted prefix. If the newest one is
	// unreadable, recovering from an older one is slower but still correct —
	// refusing to start would be worse.
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)

	if err := s.Save(testSnapshot(10, 1, "older-but-valid")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(testSnapshot(20, 2, "newest")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	newest := filepath.Join(dir, snapshotName(20, 2))
	data, err := os.ReadFile(newest)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[headerSize+2] ^= 0xff // corrupt the payload, leaving the checksum stale
	if err := os.WriteFile(newest, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Index != 10 {
		t.Errorf("Latest() index = %d, want 10 (the older valid generation)", got.Index)
	}
	if string(got.Data) != "older-but-valid" {
		t.Errorf("data = %q, want %q", got.Data, "older-but-valid")
	}
}

func TestAllSnapshotsCorruptReportsError(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)
	if err := s.Save(testSnapshot(10, 1, "only-one")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	path := filepath.Join(dir, snapshotName(10, 1))
	data, _ := os.ReadFile(path)
	data[headerSize] ^= 0xff
	os.WriteFile(path, data, 0o644)

	if _, err := s.Latest(); err == nil {
		t.Error("Latest() succeeded with every snapshot corrupt")
	} else if !strings.Contains(err.Error(), "no readable snapshot") {
		t.Errorf("error = %v, want it to say no snapshot was readable", err)
	}
}

func TestSnapshotRetention(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSnapshotStore(dir, WithKeep(2))
	if err != nil {
		t.Fatalf("NewSnapshotStore: %v", err)
	}

	for idx := raft.Index(1); idx <= 6; idx++ {
		if err := s.Save(testSnapshot(idx*10, 1, "d")); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	if got := s.Count(); got != 2 {
		t.Errorf("Count() = %d, want 2", got)
	}
	// The retained generations must be the newest ones.
	latest, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Index != 60 {
		t.Errorf("Latest() index = %d, want 60", latest.Index)
	}
}

func TestPartialSnapshotIsIgnored(t *testing.T) {
	// A crash during Save leaves a .tmp file. Because it never received its final
	// name, it can never be mistaken for a complete snapshot.
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)
	if err := s.Save(testSnapshot(10, 1, "complete")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orphan := filepath.Join(dir, snapshotName(99, 9)+tmpSuffix)
	if err := os.WriteFile(orphan, []byte("half-written garbage"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Index != 10 {
		t.Errorf("Latest() index = %d, want 10: a .tmp file must never be loaded", got.Index)
	}
	if n := len(snapshotFiles(t, dir)); n != 1 {
		t.Errorf("%d snapshot files matched the real suffix, want 1", n)
	}
}

func TestSaveLeavesNoTempFileBehind(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewSnapshotStore(dir)
	if err := s.Save(testSnapshot(5, 1, "data")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	names, err := filepath.Glob(filepath.Join(dir, "*"+tmpSuffix))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("Save left %d temporary files behind: %v", len(names), names)
	}
}

func TestSnapshotNameRoundTrip(t *testing.T) {
	idx, term, err := parseSnapshotName(snapshotName(1234, 56))
	if err != nil {
		t.Fatalf("parseSnapshotName: %v", err)
	}
	if idx != 1234 || term != 56 {
		t.Errorf("parsed (%d, %d), want (1234, 56)", idx, term)
	}
	for _, bad := range []string{"random.txt", "snap-x-1.snap", "snap-1.snap", "snap-1-x.snap"} {
		if _, _, err := parseSnapshotName(bad); err == nil {
			t.Errorf("parseSnapshotName(%q) succeeded, want an error", bad)
		}
	}
}

func TestSegmentNameRoundTrip(t *testing.T) {
	base, err := parseSegmentName(segmentName(987654321))
	if err != nil {
		t.Fatalf("parseSegmentName: %v", err)
	}
	if base != 987654321 {
		t.Errorf("parsed %d, want 987654321", base)
	}
	for _, bad := range []string{"seg-abc.wal", "notaseg.wal", "seg-1.txt"} {
		if _, err := parseSegmentName(bad); err == nil {
			t.Errorf("parseSegmentName(%q) succeeded, want an error", bad)
		}
	}
}

func TestSnapshotPayloadTruncationIsRejected(t *testing.T) {
	// Bounds-checked decoding: a payload cut short must produce an error rather
	// than a panic, whatever the length fields claim.
	full, err := encodeSnapshot(testSnapshot(9, 2, "some data here"))
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	for cut := 0; cut < len(full); cut++ {
		if _, err := decodeSnapshot(full[:cut]); err == nil {
			t.Errorf("decodeSnapshot accepted a payload truncated to %d of %d bytes", cut, len(full))
		}
	}
}

func TestSnapshotNodeSetCountBeyondPayloadIsRejected(t *testing.T) {
	// A corrupted member count must not drive an enormous allocation.
	full, err := encodeSnapshot(testSnapshot(9, 2, "d"))
	if err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	corrupt := append([]byte(nil), full...)
	// The first node-set length sits just after index and term.
	corrupt[16] = 0xff
	corrupt[17] = 0xff
	if _, err := decodeSnapshot(corrupt); err == nil {
		t.Error("decodeSnapshot accepted a node set larger than the payload")
	}
}
