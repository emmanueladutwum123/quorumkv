package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/emmanueladutwum123/quorumkv/internal/raft"
)

// recordType identifies what a record's payload holds.
type recordType uint8

const (
	recordInvalid recordType = 0
	// recordEntry is one replicated log entry.
	recordEntry recordType = 1
	// recordHardState is a term/vote/commit triple.
	recordHardState recordType = 2
	// recordSnapshotMark notes that a snapshot through some index was installed,
	// so replay knows where the log's compaction boundary sits without having to
	// read the snapshot file itself.
	recordSnapshotMark recordType = 3
)

func (t recordType) String() string {
	switch t {
	case recordEntry:
		return "entry"
	case recordHardState:
		return "hardstate"
	case recordSnapshotMark:
		return "snapshot-mark"
	default:
		return fmt.Sprintf("record(%d)", uint8(t))
	}
}

// headerSize is the fixed prefix on every record: payload length, type, and the
// checksum of the two together with the payload.
//
//	+------------+--------+--------+-------------------+
//	| length u32 | type u8| crc u32| payload (length B)|
//	+------------+--------+--------+-------------------+
//
// The length comes first so that a reader can tell whether a record is complete
// before trusting anything else about it, and the checksum covers the type byte
// as well as the payload so that a corrupted type cannot be mistaken for a valid
// record of a different kind.
const headerSize = 4 + 1 + 4

// maxRecordSize bounds a single record. Without it, a corrupted length field
// could ask the reader to allocate an arbitrary amount of memory before the
// checksum has any chance to reject it.
const maxRecordSize = 64 << 20 // 64 MiB

// crcTable uses Castagnoli, which has hardware support on both amd64 and arm64,
// so checksumming is not what limits append throughput.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord frames a payload for writing.
func encodeRecord(dst []byte, t recordType, payload []byte) []byte {
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	header[4] = byte(t)

	crc := crc32.New(crcTable)
	crc.Write(header[4:5])
	crc.Write(payload)
	binary.LittleEndian.PutUint32(header[5:9], crc.Sum32())

	dst = append(dst, header...)
	return append(dst, payload...)
}

// errTornRecord means the record at this position is incomplete or corrupt.
//
// It is deliberately not an error condition for the WAL as a whole. A torn
// record can only ever be the last one: a crash mid-write leaves a partial tail,
// and because the write was never acknowledged, nothing above the WAL was ever
// told that data was durable. Replay therefore stops cleanly at the tear rather
// than reporting corruption.
type errTornRecord struct {
	offset int64
	reason string
}

func (e *errTornRecord) Error() string {
	return fmt.Sprintf("wal: torn record at offset %d: %s", e.offset, e.reason)
}

// decodeRecord reads one record from buf, returning its type, payload, and the
// number of bytes consumed.
func decodeRecord(buf []byte, offset int64) (recordType, []byte, int, error) {
	if len(buf) < headerSize {
		return recordInvalid, nil, 0, &errTornRecord{offset, "truncated header"}
	}
	length := binary.LittleEndian.Uint32(buf[0:4])
	if length > maxRecordSize {
		// Either corruption or a file that is not a WAL. Refusing to allocate is
		// what keeps a damaged length field from becoming an out-of-memory crash.
		return recordInvalid, nil, 0, &errTornRecord{offset,
			fmt.Sprintf("implausible record length %d", length)}
	}
	total := headerSize + int(length)
	if len(buf) < total {
		return recordInvalid, nil, 0, &errTornRecord{offset, "truncated payload"}
	}

	t := recordType(buf[4])
	want := binary.LittleEndian.Uint32(buf[5:9])
	payload := buf[headerSize:total]

	crc := crc32.New(crcTable)
	crc.Write(buf[4:5])
	crc.Write(payload)
	if got := crc.Sum32(); got != want {
		return recordInvalid, nil, 0, &errTornRecord{offset,
			fmt.Sprintf("checksum mismatch: computed %08x, stored %08x", got, want)}
	}
	if t != recordEntry && t != recordHardState && t != recordSnapshotMark {
		return recordInvalid, nil, 0, &errTornRecord{offset,
			fmt.Sprintf("unknown record type %d", uint8(t))}
	}
	return t, payload, total, nil
}

// --- payload encodings -----------------------------------------------------
//
// These are hand-rolled rather than delegated to protobuf so that the on-disk
// format is fixed by this file alone. A storage format that can drift when a
// dependency changes its encoding is a format that can fail to read back data
// written by an earlier build.

const entryHeaderSize = 8 + 8 + 1 + 4

func encodeEntry(e raft.Entry) []byte {
	buf := make([]byte, entryHeaderSize+len(e.Data))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(e.Index))
	buf[16] = byte(e.Type)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(e.Data)))
	copy(buf[entryHeaderSize:], e.Data)
	return buf
}

func decodeEntry(payload []byte) (raft.Entry, error) {
	if len(payload) < entryHeaderSize {
		return raft.Entry{}, fmt.Errorf("wal: entry payload too short (%d bytes)", len(payload))
	}
	dataLen := binary.LittleEndian.Uint32(payload[17:21])
	if int(dataLen) != len(payload)-entryHeaderSize {
		return raft.Entry{}, fmt.Errorf("wal: entry data length %d does not match payload (%d bytes)",
			dataLen, len(payload)-entryHeaderSize)
	}
	e := raft.Entry{
		Term:  raft.Term(binary.LittleEndian.Uint64(payload[0:8])),
		Index: raft.Index(binary.LittleEndian.Uint64(payload[8:16])),
		Type:  raft.EntryType(payload[16]),
	}
	if dataLen > 0 {
		// Copy rather than alias: the caller's slice is a reusable read buffer.
		e.Data = make([]byte, dataLen)
		copy(e.Data, payload[entryHeaderSize:])
	}
	return e, nil
}

const hardStateSize = 8 + 8 + 8

func encodeHardState(hs raft.HardState) []byte {
	buf := make([]byte, hardStateSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(hs.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(hs.Vote))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(hs.Commit))
	return buf
}

func decodeHardState(payload []byte) (raft.HardState, error) {
	if len(payload) != hardStateSize {
		return raft.HardState{}, fmt.Errorf("wal: hard state payload is %d bytes, want %d",
			len(payload), hardStateSize)
	}
	return raft.HardState{
		Term:   raft.Term(binary.LittleEndian.Uint64(payload[0:8])),
		Vote:   raft.NodeID(binary.LittleEndian.Uint64(payload[8:16])),
		Commit: raft.Index(binary.LittleEndian.Uint64(payload[16:24])),
	}, nil
}

const snapshotMarkSize = 8 + 8

func encodeSnapshotMark(index raft.Index, term raft.Term) []byte {
	buf := make([]byte, snapshotMarkSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(index))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(term))
	return buf
}

func decodeSnapshotMark(payload []byte) (raft.Index, raft.Term, error) {
	if len(payload) != snapshotMarkSize {
		return 0, 0, fmt.Errorf("wal: snapshot mark payload is %d bytes, want %d",
			len(payload), snapshotMarkSize)
	}
	return raft.Index(binary.LittleEndian.Uint64(payload[0:8])),
		raft.Term(binary.LittleEndian.Uint64(payload[8:16])), nil
}
