// Package cold reads the S3 tier of 02-read-contract.md: LIST-based file
// discovery (§5.1, §5.5), projected parquet scans that never touch
// trace_blob on the list path (§5.4), and the pods/v1 manifests that back
// cold /pods (§2.7). It is the read-side counterpart of the seal and upload
// passes in libs/collector/hotstore.
package cold

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

// ErrNotFound is what Open and Get return for a key that a LIST surfaced but
// a compaction deleted before the read; discovery treats it as an empty
// result, not an error (02 §5.1).
var ErrNotFound = errors.New("object not found")

type (
	// ObjectInfo is the LIST projection discovery relies on: ListObjectsV2
	// returns the key and size with every entry, and both bounds of the
	// overlap test ride in the key itself (02 §5.1 step 4) — no HEAD, no
	// footer read.
	ObjectInfo struct {
		Key  string
		Size int64
	}

	// Object is one opened S3 object: random access for the parquet reader
	// (footer last, then per-column chunks).
	Object interface {
		io.ReaderAt
		io.Closer
		Size() int64
	}

	// ObjectStore is the narrow S3 surface the cold tier needs. Every
	// implementation maps a deleted-after-LIST key to ErrNotFound.
	ObjectStore interface {
		// List enumerates the objects under prefix.
		List(ctx context.Context, prefix string) ([]ObjectInfo, error)
		// Open returns a random-access handle to one object.
		Open(ctx context.Context, key string) (Object, error)
		// Get reads one small object whole (the JSON manifests of §2.7).
		Get(ctx context.Context, key string) ([]byte, error)
	}
)
