package cold

import (
	"context"
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

// ScanFile reads one discovered parquet file with column projection — the
// trace_blob and big_params_json column chunks are never read on the list
// path (02 §5.4, §2.3.2) — applies the row filter ts_ms ∈ [from, to) plus the
// §2.3 predicates and the keyset seek, and returns the surviving rows in the
// file's native (ts_ms DESC, pk ASC) order (01 §5.2), ready to be one merge
// run. A listed-but-deleted object returns an empty result (§5.1).
//
// The projection is the CallV2Projected read schema: parquet-go matches the
// file's columns to the struct by NAME and masks the chunks of columns the
// struct omits, so their pages are never fetched.
func ScanFile(ctx context.Context, store ObjectStore, ref FileRef, q model.CallsQuery, after *model.Position) ([]model.CallRow, error) {
	rows, err := readRows[storageparquet.CallV2Projected](ctx, store, ref)
	if err != nil || rows == nil {
		return nil, err
	}

	out := make([]model.CallRow, 0, len(rows))
	for i := range rows {
		row := toCallRow(&rows[i])
		if !q.Match(row) {
			continue
		}
		if after != nil && !row.Position().After(*after) {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

// FetchCall reads one call's full row — trace_blob and big_params_json
// included — from the discovered candidates. Candidates whose key-encoded
// pod-restart hash cannot match the PK are skipped without an open; the rest
// are read whole, one by one, until the PK matches (a point fetch touches the
// couple of files of one 5-minute bucket, so row-group pruning is not worth
// its weight yet). A compacted object (the reserved MaintainReplica token)
// keys its hash off the compaction's inputs, not one pod-restart, so it is a
// candidate for every PK and matched row-by-row (01 §6.6, §7). ok is false
// when no candidate holds the PK.
func FetchCall(ctx context.Context, store ObjectStore, files []FileRef, pk model.PK) (*storageparquet.CallV2, bool, error) {
	hash := model.PodRestartHash(model.PodTuple{
		Namespace: pk.PodNamespace, Service: pk.PodService,
		Pod: pk.PodName, RestartTimeMs: pk.RestartTimeMs,
	})
	for _, ref := range files {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if ref.Replica != MaintainReplica && ref.Hash != hash {
			continue // another pod-restart's write-path file, its hash cannot hold this PK
		}
		rows, err := readRows[storageparquet.CallV2](ctx, store, ref)
		if err != nil {
			return nil, false, err
		}
		for i := range rows {
			row := &rows[i]
			if row.Namespace == pk.PodNamespace && row.ServiceName == pk.PodService &&
				row.PodName == pk.PodName && row.RestartTimeMs == pk.RestartTimeMs &&
				row.TraceFileIndex == pk.TraceFileIndex && row.BufferOffset == pk.BufferOffset &&
				row.RecordIndex == pk.RecordIndex {
				return row, true, nil
			}
		}
	}
	return nil, false, nil
}

// readRows materializes every row of one object through the T read schema.
// A listed-but-deleted object returns nil rows (02 §5.1). Read errors are
// checked and wrapped — a corrupted column fails the scan instead of yielding
// zero values silently.
func readRows[T any](ctx context.Context, store ObjectStore, ref FileRef) (rows []T, err error) {
	obj, err := store.Open(ctx, ref.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil // compacted away after the LIST (02 §5.1)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", ref.Key)
	}
	defer func() { _ = obj.Close() }()

	// The library reports an unconvertible file schema (a renamed column, a
	// changed type — the non-additive changes the schema-version stamp exists
	// for) via panic; surface it as this file's scan error, not a crash.
	defer func() {
		if r := recover(); r != nil {
			rows, err = nil, errors.Errorf("read %s: %v", ref.Key, r)
		}
	}()

	// The page index and bloom filters are skipped: the scan reads whole
	// files and prunes nothing yet (see the stage1 open issue on row-group
	// pruning).
	f, err := parquet.OpenFile(obj, obj.Size(),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		if gone(ctx, store, ref.Key) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "read parquet footer of %s", ref.Key)
	}
	r := parquet.NewGenericReader[T](f)
	defer func() { _ = r.Close() }()

	rows = make([]T, r.NumRows())
	n, err := r.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		if gone(ctx, store, ref.Key) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "read %s", ref.Key)
	}
	if n != len(rows) {
		if gone(ctx, store, ref.Key) {
			return nil, nil
		}
		return nil, errors.Errorf("read %s: footer promises %d rows, read %d", ref.Key, len(rows), n)
	}
	return rows, nil
}

// gone reports whether a failed read is explained by the object having been
// deleted after the LIST — a maintain compaction past its delete-grace or a
// TTL sweep (02 §5.1 pins that case as an empty result, not an error). The
// Open-time 404 mapping covers a delete before the first byte; this covers a
// delete racing the column reads, where the store's error surfaces through
// the parquet reader and its provenance is lost. One extra round-trip, on
// the error path only.
func gone(ctx context.Context, store ObjectStore, key string) bool {
	obj, err := store.Open(ctx, key)
	if err != nil {
		return errors.Is(err, ErrNotFound)
	}
	_ = obj.Close()
	return false
}

// toCallRow maps a projected row to the merged row shape.
func toCallRow(v *storageparquet.CallV2Projected) model.CallRow {
	row := model.CallRow{
		PK: model.PK{
			PodNamespace:   v.Namespace,
			PodService:     v.ServiceName,
			PodName:        v.PodName,
			RestartTimeMs:  v.RestartTimeMs,
			TraceFileIndex: v.TraceFileIndex,
			BufferOffset:   v.BufferOffset,
			RecordIndex:    v.RecordIndex,
		},
		TsMs:           v.TsMs,
		DurationMs:     v.DurationMs,
		Method:         v.Method,
		ThreadName:     v.ThreadName,
		CpuTimeMs:      v.CpuTimeMs,
		WaitTimeMs:     v.WaitTimeMs,
		MemoryUsed:     v.MemoryUsed,
		QueueWaitMs:    v.QueueWaitMs,
		SuspendMs:      v.SuspendMs,
		ChildCalls:     v.ChildCalls,
		Transactions:   v.Transactions,
		LogsGenerated:  v.LogsGenerated,
		LogsWritten:    v.LogsWritten,
		FileRead:       v.FileRead,
		FileWritten:    v.FileWritten,
		NetRead:        v.NetRead,
		NetWritten:     v.NetWritten,
		ErrorFlag:      v.ErrorFlag,
		RetentionClass: v.RetentionClass,
		Tier:           model.TierCold,
	}
	if v.TruncatedReason != nil {
		row.TruncatedReason = *v.TruncatedReason
	}
	if len(v.Params) > 0 {
		row.Params = v.Params
	}
	return row
}
