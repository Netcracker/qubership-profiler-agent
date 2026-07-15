// Package maintain is the S3-side maintenance job of 01-write-contract.md
// §6.4 and §6.6: it compacts a settled bucket's small parquet objects into
// fewer ones and applies the per-class TTL, both decided from object keys
// alone. The job talks only to S3 — it never touches the hot store, the
// collector, or the query service — so it deploys as a stateless singleton
// (03-lifecycle.md §8). The design tolerates a second concurrent maintainer:
// the compacted key is deterministic over its inputs, so a racing PUT is
// idempotent, and PK-dedup on the read path (02-read-contract.md §6) absorbs
// the overlap window.
package maintain

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
)

// ErrNotFound maps a key that a LIST surfaced but a concurrent actor deleted
// before the read. Open reports it so a pass can abort the affected group and
// replan on the next tick.
var ErrNotFound = errors.New("object not found")

type (
	// ObjectInfo is the LIST projection a pass plans from. LastModified is
	// the S3-side write stamp: the delete-grace of 01 §6.6 is measured from
	// the compacted object's LastModified, so the wait survives a maintainer
	// restart with no local state.
	ObjectInfo struct {
		Key          string
		Size         int64
		LastModified time.Time
	}

	// Object is one opened S3 object: random access for the parquet reader
	// (footer last, then per-column chunks).
	Object interface {
		io.ReaderAt
		io.Closer
		Size() int64
	}

	// ObjectStore is the read-write S3 surface the job needs. Implementations
	// map a missing key to ErrNotFound on Open, and treat Delete of a missing
	// key as success — a pass cut short re-runs its deletes.
	ObjectStore interface {
		// List enumerates the objects under prefix.
		List(ctx context.Context, prefix string) ([]ObjectInfo, error)
		// Open returns a random-access handle to one object.
		Open(ctx context.Context, key string) (Object, error)
		// Put writes one object whole.
		Put(ctx context.Context, key string, body []byte) error
		// Delete removes one object; a missing key is not an error.
		Delete(ctx context.Context, key string) error
	}
)
