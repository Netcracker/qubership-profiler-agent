package hotstore

import (
	"os"

	"github.com/pkg/errors"
)

// blobBuffer accumulates one call's blob during the seal walk. It starts in
// RAM and spills to a temp file under the seal scratch dir once it outgrows
// the per-call limit, so the pass's peak memory stays bounded by the calls
// open across the segment cursor (01-write-contract.md §6.5). A crashed seal
// leaves the spill files in parquet-sealing/, which recovery discards.
type blobBuffer struct {
	dir   string
	limit int64
	data  []byte
	file  *os.File
	size  int64
}

// newBlobBuffer opens the buffer with the 8-byte timerStartTime prefix that
// frames every blob (01-write-contract.md §4.5).
func newBlobBuffer(dir string, limit int64, prefix []byte) *blobBuffer {
	return &blobBuffer{dir: dir, limit: limit, data: append([]byte(nil), prefix...), size: int64(len(prefix))}
}

func (b *blobBuffer) Append(p []byte) error {
	if b.file == nil && b.size+int64(len(p)) > b.limit {
		f, err := os.CreateTemp(b.dir, "blob-*.tmp")
		if err != nil {
			return errors.Wrap(err, "create blob spill file")
		}
		if _, err := f.Write(b.data); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return errors.Wrap(err, "spill blob")
		}
		b.file = f
		b.data = nil
	}
	if b.file != nil {
		_, err := b.file.Write(p)
		b.size += int64(len(p))
		return errors.Wrap(err, "spill blob")
	}
	b.data = append(b.data, p...)
	b.size += int64(len(p))
	return nil
}

// Bytes returns the assembled blob, reading a spilled buffer back from disk.
func (b *blobBuffer) Bytes() ([]byte, error) {
	if b.file == nil {
		return b.data, nil
	}
	out := make([]byte, b.size)
	if _, err := b.file.ReadAt(out, 0); err != nil {
		return nil, errors.Wrap(err, "read spilled blob")
	}
	return out, nil
}

// Spilled reports whether the buffer overflowed to disk.
func (b *blobBuffer) Spilled() bool { return b.file != nil }

// Free releases the buffer and removes its spill file. Idempotent.
func (b *blobBuffer) Free() {
	if b.file != nil {
		name := b.file.Name()
		_ = b.file.Close()
		_ = os.Remove(name)
		b.file = nil
	}
	b.data = nil
	b.size = 0
}
