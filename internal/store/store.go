// Package store implements the replicated state machine: a key-value map plus
// the client session table that makes retries safe.
package store

import (
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	kvv1 "github.com/emmanueladutwum123/quorumkv/internal/gen/kvv1"
	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// Store is the deterministic state machine behind the replicated log.
//
// Determinism is the requirement everything else follows from: every replica
// applies the same commands in the same order and must reach byte-identical
// state, or a snapshot taken on one node would not be valid on another. That is
// why iteration over the map is always sorted before it is serialised, and why
// nothing here consults a clock, a random source, or the network.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte

	// sessions maps a client to the last request it had applied, with the result.
	//
	// Raft guarantees a committed entry is applied at least once, not exactly
	// once: a client whose response is lost retries, and the same command reaches
	// the log twice. For a blind Put that is harmless, but a duplicated
	// compare-and-swap can succeed twice against different states. Recognising
	// the repeat and returning the original answer is what makes the API
	// idempotent.
	sessions map[uint64]*session

	// applied is the log index this state reflects, so a snapshot can be labelled
	// with the position it corresponds to.
	applied raft.Index
}

type session struct {
	sequence uint64
	result   *kvv1.CommandResult
}

// New returns an empty store.
func New() *Store {
	return &Store{
		data:     make(map[string][]byte),
		sessions: make(map[uint64]*session),
	}
}

// Apply executes a committed log entry and returns its result.
//
// Entries the consensus layer interprets itself — no-ops and configuration
// changes — carry no state machine effect and are accounted for but not decoded.
func (s *Store) Apply(e raft.Entry) (*kvv1.CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.Index > s.applied {
		s.applied = e.Index
	}
	if e.Type != raft.EntryNormal || len(e.Data) == 0 {
		return &kvv1.CommandResult{}, nil
	}

	var cmd kvv1.Command
	if err := proto.Unmarshal(e.Data, &cmd); err != nil {
		// A command that cannot be decoded is a determinism hazard: replicas that
		// disagree about whether an entry had an effect diverge. Applying entries
		// is not the place to be tolerant, so this is reported and the caller
		// treats it as fatal.
		return nil, fmt.Errorf("store: decode command at index %d: %w", e.Index, err)
	}

	if hdr := cmd.GetHeader(); hdr != nil && hdr.GetClientId() != 0 {
		if cached, ok := s.replayed(hdr.GetClientId(), hdr.GetSequence()); ok {
			return cached, nil
		}
	}

	result := s.execute(&cmd)

	if hdr := cmd.GetHeader(); hdr != nil && hdr.GetClientId() != 0 {
		s.sessions[hdr.GetClientId()] = &session{sequence: hdr.GetSequence(), result: result}
	}
	return result, nil
}

// replayed reports whether this exact request has already been applied, along
// with the answer it produced.
func (s *Store) replayed(clientID, sequence uint64) (*kvv1.CommandResult, bool) {
	sess, ok := s.sessions[clientID]
	if !ok {
		return nil, false
	}
	if sess.sequence != sequence {
		// Only the most recent request per client is remembered. A client sends
		// requests one at a time and a retry always repeats the latest, so a
		// mismatch means this is a new request rather than a replay.
		return nil, false
	}
	return sess.result, true
}

func (s *Store) execute(cmd *kvv1.Command) *kvv1.CommandResult {
	key := string(cmd.GetKey())

	switch cmd.GetOp() {
	case kvv1.OpType_OP_TYPE_PUT:
		s.data[key] = append([]byte(nil), cmd.GetValue()...)
		return &kvv1.CommandResult{}

	case kvv1.OpType_OP_TYPE_DELETE:
		_, existed := s.data[key]
		delete(s.data, key)
		return &kvv1.CommandResult{Existed: existed}

	case kvv1.OpType_OP_TYPE_CAS:
		current, found := s.data[key]
		ok := false
		if cmd.GetExpectAbsent() {
			ok = !found
		} else {
			ok = found && string(current) == string(cmd.GetExpectedValue())
		}
		if !ok {
			// A failed swap reports what was actually there, so the caller needs no
			// follow-up read — and a follow-up read would be a separate operation
			// that could observe a different value anyway.
			return &kvv1.CommandResult{
				Swapped:      false,
				Found:        found,
				CurrentValue: append([]byte(nil), current...),
			}
		}
		// For a compare-and-swap, Value carries the replacement.
		s.data[key] = append([]byte(nil), cmd.GetValue()...)
		return &kvv1.CommandResult{
			Swapped:      true,
			Found:        found,
			CurrentValue: append([]byte(nil), cmd.GetValue()...),
		}

	default:
		// An unknown opcode is what a rolling upgrade looks like from the old
		// build's side. Ignoring it keeps every replica in agreement that the
		// entry had no effect, which is the only deterministic option available.
		return &kvv1.CommandResult{}
	}
}

// Get reads a key from local state. Whether that read is linearizable is decided
// above this layer: the server only calls it after the consensus core has
// confirmed a read index, or when the caller asked for a stale read.
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Len reports the number of keys held.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// AppliedIndex reports the log index this state reflects.
func (s *Store) AppliedIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Snapshot serialises the whole state machine.
//
// Keys and sessions are emitted in sorted order so that two replicas holding
// identical state produce identical bytes. Without that, snapshots would differ
// between nodes for no reason, which makes them impossible to compare and any
// checksum over them meaningless.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	snap := &kvv1.StoreSnapshot{Entries: make([]*kvv1.KeyValue, 0, len(keys))}
	for _, k := range keys {
		snap.Entries = append(snap.Entries, &kvv1.KeyValue{
			Key:   []byte(k),
			Value: append([]byte(nil), s.data[k]...),
		})
	}

	clients := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		clients = append(clients, id)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	for _, id := range clients {
		sess := s.sessions[id]
		snap.Sessions = append(snap.Sessions, &kvv1.Session{
			ClientId: id,
			Sequence: sess.sequence,
			Result:   sess.result,
		})
	}

	// Deterministic marshalling, for the same reason the ordering above is fixed.
	return proto.MarshalOptions{Deterministic: true}.Marshal(snap)
}

// Restore replaces the entire state machine with a snapshot's contents.
//
// Replacement is wholesale rather than merged: the snapshot is a complete
// description of committed state, and anything currently held that it does not
// mention was either uncommitted or already superseded.
func (s *Store) Restore(data []byte, index raft.Index) error {
	var snap kvv1.StoreSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("store: decode snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string][]byte, len(snap.GetEntries()))
	for _, kv := range snap.GetEntries() {
		s.data[string(kv.GetKey())] = append([]byte(nil), kv.GetValue()...)
	}
	s.sessions = make(map[uint64]*session, len(snap.GetSessions()))
	for _, sess := range snap.GetSessions() {
		s.sessions[sess.GetClientId()] = &session{
			sequence: sess.GetSequence(),
			result:   sess.GetResult(),
		}
	}
	s.applied = index
	return nil
}

// EncodeCommand marshals a command for the log.
func EncodeCommand(cmd *kvv1.Command) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(cmd)
}
