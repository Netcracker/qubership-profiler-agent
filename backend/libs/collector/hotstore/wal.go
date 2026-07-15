package hotstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Wal is an append-only write-ahead log per 01-write-contract.md §3.2-§3.3:
// length-prefixed records, fsync every N records or T elapsed (whichever
// first), and a CRC32 footer written on clean close. A zero-length record
// marks the footer, so replay can tell a footer from a truncated tail; the
// contract does not pin the footer bytes.
type Wal struct {
	mu   sync.Mutex
	f    *os.File
	crc  uint32 // running CRC32 over every record byte written
	size int64  // bytes appended so far (next record's offset)

	fsyncRecords  int
	fsyncInterval time.Duration
	unsynced      int
	lastSync      time.Time
}

// OpenWal creates (or opens for append) the WAL at path. A WAL is only ever
// appended to within one collector lifetime: on restart the pod-restart is
// closed and its WAL is replayed, never extended (03-lifecycle.md §3.3).
func OpenWal(path string, fsyncRecords int, fsyncInterval time.Duration) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "open wal")
	}
	return &Wal{
		f:             f,
		crc:           0,
		fsyncRecords:  fsyncRecords,
		fsyncInterval: fsyncInterval,
		lastSync:      time.Now(),
	}, nil
}

// Append writes one record and returns its byte offset within the WAL.
func (w *Wal) Append(body []byte) (offset int64, err error) {
	if len(body) == 0 {
		return 0, errors.New("empty wal record: zero length marks the footer")
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	offset = w.size
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(body)))
	if _, err = w.f.Write(lenBuf[:n]); err != nil {
		return 0, errors.Wrap(err, "wal append")
	}
	if _, err = w.f.Write(body); err != nil {
		return 0, errors.Wrap(err, "wal append")
	}
	w.crc = crc32.Update(w.crc, crc32.IEEETable, lenBuf[:n])
	w.crc = crc32.Update(w.crc, crc32.IEEETable, body)
	w.size += int64(n + len(body))

	w.unsynced++
	if w.unsynced >= w.fsyncRecords || time.Since(w.lastSync) >= w.fsyncInterval {
		if err = w.sync(); err != nil {
			return 0, err
		}
	}
	return offset, nil
}

// Sync forces an fsync regardless of the periodic policy.
func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *Wal) sync() error {
	if w.unsynced == 0 {
		return nil
	}
	if err := w.f.Sync(); err != nil {
		return errors.Wrap(err, "wal fsync")
	}
	w.unsynced = 0
	w.lastSync = time.Now()
	return nil
}

// Close writes the footer (a zero-length record followed by the 4-byte CRC32
// of everything before it), fsyncs, and closes the file.
func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	footer := make([]byte, 0, 5)
	footer = append(footer, 0) // zero varint length = footer marker
	footer = binary.BigEndian.AppendUint32(footer, w.crc)
	if _, err := w.f.Write(footer); err != nil {
		return errors.Wrap(err, "wal footer")
	}
	if err := w.f.Sync(); err != nil {
		return errors.Wrap(err, "wal fsync")
	}
	err := w.f.Close()
	w.f = nil
	return errors.Wrap(err, "wal close")
}

// ReplayWal reads records from the WAL at path, calling apply for each with
// its offset and body. A structurally invalid tail — a torn record from a
// crash — is truncated in place and replay succeeds with what precedes it; a
// present-but-mismatching CRC footer fails the replay (01-write-contract.md
// §3.2). clean reports whether a verified footer was found.
func ReplayWal(path string, apply func(offset int64, body []byte) error) (clean bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, errors.Wrap(err, "read wal")
	}

	crc := uint32(0)
	pos := 0
	for pos < len(data) {
		recLen, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			break // torn varint at the tail
		}
		if recLen == 0 {
			// Footer marker: 4-byte CRC32 must follow and close the file.
			if len(data)-pos-n != 4 {
				break // torn footer
			}
			stored := binary.BigEndian.Uint32(data[pos+n:])
			if stored != crc {
				return false, fmt.Errorf("wal %s: footer CRC mismatch: stored %08x, computed %08x", path, stored, crc)
			}
			return true, nil
		}
		end := pos + n + int(recLen)
		if recLen > uint64(len(data)) || end > len(data) {
			break // torn record body at the tail
		}
		if err := apply(int64(pos), data[pos+n:end]); err != nil {
			return false, err
		}
		crc = crc32.Update(crc, crc32.IEEETable, data[pos:end])
		pos = end
	}

	if pos < len(data) {
		// Standard WAL tail-corruption recovery: drop the torn tail.
		if err := os.Truncate(path, int64(pos)); err != nil {
			return false, errors.Wrap(err, "truncate wal tail")
		}
	}
	return false, nil
}

var _ io.Closer = (*Wal)(nil)
