package raft

import (
	"errors"
	"testing"
)

// ents is a terse builder for a log's worth of entries, where each argument is
// the term of the entry at index 1, 2, 3, ... Raft bugs are almost always about
// term/index relationships, so tests read better when a log is expressible as
// its term sequence.
func ents(terms ...Term) []Entry {
	out := make([]Entry, len(terms))
	for i, t := range terms {
		out[i] = Entry{Term: t, Index: Index(i + 1), Type: EntryNormal}
	}
	return out
}

func logWith(terms ...Term) *raftLog {
	l := newLog(0, 0)
	l.append(ents(terms...)...)
	return l
}

func TestLogEmptyBounds(t *testing.T) {
	l := newLog(0, 0)
	if got := l.firstIndex(); got != 1 {
		t.Errorf("firstIndex() = %d, want 1", got)
	}
	if got := l.lastIndex(); got != 0 {
		t.Errorf("lastIndex() = %d, want 0", got)
	}
	if got := l.lastTerm(); got != 0 {
		t.Errorf("lastTerm() = %d, want 0", got)
	}
}

func TestLogTermLookup(t *testing.T) {
	// Boundary at (3, 2): indexes 1-3 are compacted, 4-6 are present.
	l := newLog(3, 2)
	l.append(Entry{Term: 2, Index: 4}, Entry{Term: 5, Index: 5}, Entry{Term: 5, Index: 6})

	tests := []struct {
		index   Index
		want    Term
		wantErr error
	}{
		{1, 0, ErrCompacted},
		{3, 2, nil}, // the boundary itself must remain answerable
		{4, 2, nil},
		{5, 5, nil},
		{6, 5, nil},
		{7, 0, ErrUnavailable},
	}
	for _, tt := range tests {
		got, err := l.term(tt.index)
		if !errors.Is(err, tt.wantErr) {
			t.Errorf("term(%d) error = %v, want %v", tt.index, err, tt.wantErr)
		}
		if got != tt.want {
			t.Errorf("term(%d) = %d, want %d", tt.index, got, tt.want)
		}
	}
}

func TestLogIsUpToDate(t *testing.T) {
	// Local log: terms [1, 2, 2], so last is (index 3, term 2).
	l := logWith(1, 2, 2)

	tests := []struct {
		name     string
		lastIdx  Index
		lastTerm Term
		want     bool
	}{
		{"identical log", 3, 2, true},
		{"longer, same term", 4, 2, true},
		{"shorter, same term", 2, 2, false},
		{"higher term wins despite shorter log", 1, 3, true},
		{"lower term loses despite longer log", 9, 1, false},
	}
	for _, tt := range tests {
		if got := l.isUpToDate(tt.lastIdx, tt.lastTerm); got != tt.want {
			t.Errorf("%s: isUpToDate(%d, %d) = %v, want %v", tt.name, tt.lastIdx, tt.lastTerm, got, tt.want)
		}
	}
}

func TestLogMaybeAppendRejectsFailedConsistencyCheck(t *testing.T) {
	l := logWith(1, 2, 3)

	// Right index, wrong term: the follower must refuse rather than append.
	if _, ok := l.maybeAppend(3, 2, 3, ents(1, 2, 3, 4)[3:]); ok {
		t.Fatal("maybeAppend accepted a mismatched prevLogTerm")
	}
	// Index beyond the end of the log.
	if _, ok := l.maybeAppend(9, 3, 3, nil); ok {
		t.Fatal("maybeAppend accepted a prevLogIndex past the end of the log")
	}
	if got := l.lastIndex(); got != 3 {
		t.Errorf("log was modified by a rejected append: lastIndex() = %d, want 3", got)
	}
}

func TestLogMaybeAppendTruncatesConflictingSuffix(t *testing.T) {
	l := logWith(1, 2, 2, 2)
	l.commitTo(2)

	// A new leader in term 5 overwrites the uncommitted tail from index 3.
	newEnts := []Entry{{Term: 5, Index: 3}, {Term: 5, Index: 4}}
	last, ok := l.maybeAppend(2, 2, 2, newEnts)
	if !ok {
		t.Fatal("maybeAppend rejected a valid append")
	}
	if last != 4 {
		t.Errorf("last new index = %d, want 4", last)
	}
	for i, want := range map[Index]Term{1: 1, 2: 2, 3: 5, 4: 5} {
		if got, _ := l.term(i); got != want {
			t.Errorf("after truncation term(%d) = %d, want %d", i, got, want)
		}
	}
}

func TestLogMaybeAppendIsIdempotent(t *testing.T) {
	l := logWith(1, 2, 2)
	l.commitTo(3)

	// Retransmission of already-present entries must not truncate: these are
	// committed, and discarding them would lose acknowledged writes.
	last, ok := l.maybeAppend(1, 1, 3, []Entry{{Term: 2, Index: 2}, {Term: 2, Index: 3}})
	if !ok {
		t.Fatal("maybeAppend rejected a duplicate append")
	}
	if last != 3 {
		t.Errorf("last new index = %d, want 3", last)
	}
	if got := l.lastIndex(); got != 3 {
		t.Errorf("lastIndex() = %d, want 3", got)
	}
	if l.committed != 3 {
		t.Errorf("committed = %d, want 3", l.committed)
	}
}

func TestLogMaybeAppendClampsCommitToLastNewEntry(t *testing.T) {
	l := logWith(1, 1)

	// The leader reports commit=9 but only sends up to index 3. Committing
	// beyond what this node holds would let it apply and serve an entry it has
	// never actually seen.
	last, ok := l.maybeAppend(2, 1, 9, []Entry{{Term: 1, Index: 3}})
	if !ok {
		t.Fatal("maybeAppend rejected a valid append")
	}
	if last != 3 {
		t.Errorf("last new index = %d, want 3", last)
	}
	if l.committed != 3 {
		t.Errorf("committed = %d, want 3 (clamped to last new entry)", l.committed)
	}
}

func TestLogMaybeAppendPanicsOnCommittedConflict(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic when a committed entry is contradicted")
		}
	}()
	l := logWith(1, 2, 2)
	l.commitTo(3)
	l.maybeAppend(1, 1, 3, []Entry{{Term: 9, Index: 2}})
}

func TestLogFindConflictByTermSkipsWholeTerm(t *testing.T) {
	// terms: idx1=1, idx2..4=5, idx5=6
	l := logWith(1, 5, 5, 5, 6)

	tests := []struct {
		name      string
		index     Index
		term      Term
		wantIndex Index
		wantTerm  Term
	}{
		// Probing at index 5 with term 5 walks past the single term-6 entry.
		{"skips one entry", 5, 5, 4, 5},
		// Term 1 forces a walk back over the entire term-5 run in one call.
		{"skips whole term run", 5, 1, 1, 1},
		// A term at or above the local entry stops immediately.
		{"no walk needed", 5, 6, 5, 6},
		// An index past the end is clamped to the log's last entry first.
		{"clamps past end", 99, 6, 5, 6},
	}
	for _, tt := range tests {
		gotIdx, gotTerm := l.findConflictByTerm(tt.index, tt.term)
		if gotIdx != tt.wantIndex || gotTerm != tt.wantTerm {
			t.Errorf("%s: findConflictByTerm(%d, %d) = (%d, %d), want (%d, %d)",
				tt.name, tt.index, tt.term, gotIdx, gotTerm, tt.wantIndex, tt.wantTerm)
		}
	}
}

func TestLogTruncationRewindsDurability(t *testing.T) {
	l := logWith(1, 1, 1, 1)
	l.stableTo(4)

	l.truncateFrom(3)

	if l.lastIndex() != 2 {
		t.Errorf("lastIndex() = %d, want 2", l.lastIndex())
	}
	// Entries 3 and 4 were reported durable but are no longer in this log. If
	// stable stayed at 4, a crash could resurrect them from the WAL.
	if l.stable != 2 {
		t.Errorf("stable = %d, want 2 after truncation", l.stable)
	}
	if got := l.unstableEntries(); len(got) != 0 {
		t.Errorf("unstableEntries() = %v, want empty", got)
	}
}

func TestLogUnstableEntriesTracksDurability(t *testing.T) {
	l := logWith(1, 1, 2)

	if got := l.unstableEntries(); len(got) != 3 {
		t.Fatalf("unstableEntries() returned %d entries, want 3", len(got))
	}
	l.stableTo(2)
	got := l.unstableEntries()
	if len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("unstableEntries() = %v, want only index 3", got)
	}
	l.stableTo(3)
	if got := l.unstableEntries(); len(got) != 0 {
		t.Errorf("unstableEntries() = %v, want empty", got)
	}
}

func TestLogSliceReturnsIndependentCopy(t *testing.T) {
	l := logWith(1, 1, 1)
	got, err := l.slice(1, 4, 0)
	if err != nil {
		t.Fatalf("slice: %v", err)
	}
	got[0].Term = 99
	if term, _ := l.term(1); term != 1 {
		t.Error("mutating a slice result corrupted the log")
	}
}

func TestLogSliceRespectsLimitAndBounds(t *testing.T) {
	l := newLog(2, 1)
	l.append(Entry{Term: 1, Index: 3}, Entry{Term: 1, Index: 4}, Entry{Term: 1, Index: 5})

	if got, err := l.slice(3, 6, 2); err != nil || len(got) != 2 {
		t.Errorf("slice(3, 6, 2) = %v, %v; want 2 entries", got, err)
	}
	if _, err := l.slice(2, 4, 0); !errors.Is(err, ErrCompacted) {
		t.Errorf("slice below boundary error = %v, want ErrCompacted", err)
	}
	if _, err := l.slice(3, 9, 0); !errors.Is(err, ErrUnavailable) {
		t.Errorf("slice past end error = %v, want ErrUnavailable", err)
	}
	if got, err := l.slice(4, 4, 0); err != nil || got != nil {
		t.Errorf("empty slice = %v, %v; want nil, nil", got, err)
	}
}

func TestLogCompact(t *testing.T) {
	l := logWith(1, 2, 2, 3)
	l.commitTo(4)
	l.appliedTo(3)

	// Compacting past the applied index would discard entries the state
	// machine has not consumed.
	if err := l.compact(4); err == nil {
		t.Error("compact past applied index succeeded, want error")
	}
	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	if l.firstIndex() != 4 {
		t.Errorf("firstIndex() = %d, want 4", l.firstIndex())
	}
	if l.snapTerm != 2 {
		t.Errorf("snapTerm = %d, want 2", l.snapTerm)
	}
	// The boundary must still answer the consistency check.
	if !l.matchTerm(3, 2) {
		t.Error("matchTerm at the compaction boundary failed")
	}
	if got, _ := l.term(4); got != 3 {
		t.Errorf("term(4) = %d, want 3 (survived compaction)", got)
	}
	if err := l.compact(3); !errors.Is(err, ErrCompacted) {
		t.Errorf("re-compact error = %v, want ErrCompacted", err)
	}
}

func TestLogRestore(t *testing.T) {
	l := logWith(1, 1, 1)
	l.commitTo(1)

	if err := l.restore(Snapshot{Index: 5, Term: 4}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if l.firstIndex() != 6 || l.lastIndex() != 5 {
		t.Errorf("bounds after restore = [%d, %d], want [6, 5]", l.firstIndex(), l.lastIndex())
	}
	for _, f := range []struct {
		name string
		got  Index
	}{{"committed", l.committed}, {"applied", l.applied}, {"stable", l.stable}} {
		if f.got != 5 {
			t.Errorf("%s = %d, want 5", f.name, f.got)
		}
	}
	if got := l.lastTerm(); got != 4 {
		t.Errorf("lastTerm() = %d, want 4", got)
	}
}

func TestLogRestoreRejectsStaleSnapshot(t *testing.T) {
	l := newLog(5, 4)
	if err := l.restore(Snapshot{Index: 3, Term: 2}); !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Errorf("restore of older snapshot = %v, want ErrSnapshotOutOfDate", err)
	}
	if l.snapIndex != 5 {
		t.Errorf("snapIndex = %d, want 5 (unchanged)", l.snapIndex)
	}
}

func TestLogRestoreKeepsRicherLocalLog(t *testing.T) {
	// The local log already covers index 3 at term 2 and extends past it.
	l := logWith(1, 2, 2, 2, 2)

	err := l.restore(Snapshot{Index: 3, Term: 2})
	if !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Errorf("restore = %v, want ErrSnapshotOutOfDate for a covered snapshot", err)
	}
	if l.lastIndex() != 5 {
		t.Errorf("lastIndex() = %d, want 5: the longer local log must be kept", l.lastIndex())
	}
	if l.committed != 3 {
		t.Errorf("committed = %d, want 3: the snapshot still proves commitment", l.committed)
	}
}

func TestLogNextCommittedAndApply(t *testing.T) {
	l := logWith(1, 1, 1, 1)
	l.commitTo(3)

	if !l.hasPendingApply() {
		t.Fatal("hasPendingApply() = false, want true")
	}
	got := l.nextCommitted(2)
	if len(got) != 2 || got[0].Index != 1 || got[1].Index != 2 {
		t.Fatalf("nextCommitted(2) = %v, want indexes 1 and 2", got)
	}
	l.appliedTo(3)
	if l.hasPendingApply() {
		t.Error("hasPendingApply() = true after applying everything committed")
	}
	if got := l.nextCommitted(0); got != nil {
		t.Errorf("nextCommitted(0) = %v, want nil", got)
	}
}

func TestLogCommitToNeverRegresses(t *testing.T) {
	l := logWith(1, 1, 1)
	l.commitTo(3)
	l.commitTo(1) // a stale or duplicated message
	if l.committed != 3 {
		t.Errorf("committed = %d, want 3: commit index must not move backwards", l.committed)
	}
}

func TestLogAppendGapPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic when appending a non-contiguous entry")
		}
	}()
	l := logWith(1, 1)
	l.append(Entry{Term: 1, Index: 7})
}
