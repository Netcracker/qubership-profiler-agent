package cold

import (
	"context"
	"io"
	"unsafe"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

// batchConsumer receives one decoded batch under its still-held lease: the
// row-group index, the batch's first row offset within that row group, and
// the rows. Whatever must outlive the batch is deep-copied and transferred
// out before returning; the caller releases the lease right after. stop=true
// ends the scan early (the point fetch found its row).
type batchConsumer[T any] func(ctx context.Context, rgIndex int, rowOffset int64, rows []T, lease *budget.Lease) (stop bool, err error)

// scanBatches streams one parquet object through the projection T in
// budget-charged batches (02 §7.5): per row group, batches of K rows are
// charged from the footer estimate before decoding, reconciled to the actual
// footprint after, handed to consume, and released. The whole file is never
// materialized. It preserves the prior readRows semantics: a
// listed-but-deleted object is an empty result (02 §5.1), a schema panic
// from the library becomes this file's scan error, and a row count short of
// the footer promise is an error.
//
// The file opens with SkipPageIndex — neither the list scan nor the PK scan
// prunes pages; the surgical column read of the point fetch opens its own
// handle WITH the index (§7.5).
func scanBatches[T any](ctx context.Context, store ObjectStore, ref FileRef, b *budget.Budget,
	footprint func(*T) int64, overrun func(), consume batchConsumer[T]) (err error) {

	obj, err := store.Open(ctx, ref.Key)
	if errors.Is(err, ErrNotFound) {
		return nil // compacted away after the LIST (02 §5.1)
	}
	if err != nil {
		return errors.Wrapf(err, "open %s", ref.Key)
	}
	defer func() { _ = obj.Close() }()

	// The library reports an unconvertible file schema (a renamed column, a
	// changed type — the non-additive changes the schema-version stamp exists
	// for) via panic; surface it as this file's scan error, not a crash.
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("read %s: %v", ref.Key, r)
		}
	}()

	f, err := parquet.OpenFile(obj, obj.Size(),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		if gone(ctx, store, ref.Key) {
			return nil
		}
		return errors.Wrapf(err, "read parquet footer of %s", ref.Key)
	}

	var zero T
	cols := chargedColumns(parquet.SchemaOf(&zero))
	structSize := int64(unsafe.Sizeof(zero))
	meta := f.Metadata()

	for rgIndex, rg := range f.RowGroups() {
		perRow := perRowEstimate(&meta.RowGroups[rgIndex], cols, structSize)
		k := batchRows(perRow)
		stop, err := scanRowGroup(ctx, store, ref, b, footprint, overrun, consume,
			rgIndex, rg, perRow, k)
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// scanRowGroup runs the charged batch loop over one row group.
func scanRowGroup[T any](ctx context.Context, store ObjectStore, ref FileRef, b *budget.Budget,
	footprint func(*T) int64, overrun func(), consume batchConsumer[T],
	rgIndex int, rg parquet.RowGroup, perRow int64, k int) (stop bool, err error) {

	r := parquet.NewGenericRowGroupReader[T](rg)
	defer func() { _ = r.Close() }()

	buf := make([]T, k)
	var zero T
	rowOffset := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		// Charge only the rows this batch can actually produce: the batch
		// size or the row group's remainder, whichever is smaller — a
		// 40-row file must not pay for a 1024-row batch.
		batch := buf
		if remaining := rg.NumRows() - rowOffset; remaining < int64(len(batch)) {
			if remaining <= 0 {
				return false, nil
			}
			batch = buf[:remaining]
		}
		charge := int64(len(batch)) * perRow * chargeSafety
		lease, err := b.Acquire(ctx, charge)
		if err != nil {
			return false, errors.Wrapf(err, "scan %s", ref.Key)
		}
		// The buffer is reused across batches; stale values from the previous
		// batch must not leak into rows the reader leaves untouched.
		for i := range batch {
			batch[i] = zero
		}
		n, readErr := r.Read(batch)
		if n > 0 {
			var actual int64
			for i := 0; i < n; i++ {
				actual += footprint(&batch[i])
			}
			if actual > charge {
				// The estimate undershot (skewed rows); true the ledger up.
				// A denial here aborts the scan like any other charge denial.
				if err := lease.Grow(ctx, actual-charge); err != nil {
					lease.Release()
					return false, errors.Wrapf(err, "scan %s", ref.Key)
				}
				if overrun != nil {
					overrun()
				}
			} else {
				lease.Shrink(charge - actual)
			}
			stop, cerr := consume(ctx, rgIndex, rowOffset, batch[:n], lease)
			lease.Release()
			if cerr != nil {
				return false, cerr
			}
			if stop {
				return true, nil
			}
			rowOffset += int64(n)
		} else {
			lease.Release()
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				// Preserve the old readRows integrity check: a clean EOF short
				// of the row group's footer-promised row count is a truncated
				// file. If the object vanished mid-read it is the not-found
				// case (an empty result, 02 §5.1); otherwise the short read
				// fails this file's scan rather than silently dropping its tail
				// and advancing the cursor past the rows it never returned.
				if rowOffset < rg.NumRows() {
					if gone(ctx, store, ref.Key) {
						return false, nil
					}
					return false, errors.Errorf("read %s: row group %d promises %d rows, read %d",
						ref.Key, rgIndex, rg.NumRows(), rowOffset)
				}
				return false, nil
			}
			if gone(ctx, store, ref.Key) {
				return false, nil
			}
			return false, errors.Wrapf(readErr, "read %s", ref.Key)
		}
	}
}

// chargedColumns lists the top-level column names of the projection schema.
// Matching on the first path element (not the full leaf path) folds a nested
// column — the params MAP and its key/value leaves — into one charged name.
func chargedColumns(schema *parquet.Schema) map[string]bool {
	cols := make(map[string]bool)
	for _, path := range schema.Columns() {
		if len(path) > 0 {
			cols[path[0]] = true
		}
	}
	return cols
}
