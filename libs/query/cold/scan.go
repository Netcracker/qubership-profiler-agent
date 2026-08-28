package cold

import (
	"context"
	"io"
	"sort"
	"unsafe"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/pkg/errors"
)

// TraceColumns and TreeColumns are the surgical column sets of the two point
// endpoints (02 §7.5): the blob and its truncation marker for /trace, plus
// the self-contained-row columns for /tree.
var (
	TraceColumns = []string{"trace_blob", "truncated_reason"}
	TreeColumns  = []string{"trace_blob", "truncated_reason", "big_params_json", "dict_words_json", "suspend_json"}
)

// ScanFile reads one discovered parquet file with column projection — the
// trace_blob and big_params_json column chunks are never read on the list
// path (02 §5.4, §2.3.2) — applies the row filter ts_ms ∈ [from, to) plus the
// §2.3 predicates and the keyset seek, and returns the surviving rows in the
// file's native (ts_ms DESC, pk ASC) order (01 §5.2), ready to be one merge
// run. A listed-but-deleted object returns an empty result (§5.1).
//
// This is the unbudgeted compatibility surface for tests and tools; the
// budgeted production path is Source.Calls, which streams the same batches
// against the read budget and caps what it retains (02 §7.5).
func ScanFile(ctx context.Context, store ObjectStore, ref FileRef, q model.CallsQuery, after *model.Position) ([]model.CallRow, error) {
	var out []model.CallRow
	err := scanBatches(ctx, store, ref, nil, projectedFootprint, nil,
		func(_ context.Context, _ int, _ int64, rows []storageparquet.CallV2Projected, _ *budget.Lease) (bool, error) {
			for i := range rows {
				row := toCallRow(&rows[i])
				if !q.Match(row) {
					continue
				}
				if after != nil && !row.Position().After(*after) {
					continue
				}
				out = append(out, copyCallRow(&row))
			}
			return false, nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// projectedFootprint is the reconcile model for list-projection rows; it
// mirrors callRowFootprint on the pre-conversion shape.
func projectedFootprint(v *storageparquet.CallV2Projected) int64 {
	row := toCallRow(v)
	return callRowFootprint(&row)
}

// pkScanRow is the point-fetch phase-1 projection (02 §7.5): the seven PK
// components and nothing else, so the PK search never decodes a blob column.
// truncated_reason deliberately stays out — decoding it for every row is
// waste; the surgical phase reads it for the one matched row.
type pkScanRow struct {
	Namespace      string `parquet:"namespace,dict"`
	ServiceName    string `parquet:"service_name,dict"`
	PodName        string `parquet:"pod_name,dict"`
	RestartTimeMs  int64  `parquet:"restart_time_ms"`
	TraceFileIndex int32  `parquet:"trace_file_index"`
	BufferOffset   int32  `parquet:"buffer_offset"`
	RecordIndex    int32  `parquet:"record_index"`
}

func pkScanFootprint(r *pkScanRow) int64 {
	return int64(unsafe.Sizeof(pkScanRow{})) + containerOverhead +
		int64(len(r.Namespace)+len(r.ServiceName)+len(r.PodName))
}

func (r *pkScanRow) matches(pk model.PK) bool {
	return r.Namespace == pk.PodNamespace && r.ServiceName == pk.PodService &&
		r.PodName == pk.PodName && r.RestartTimeMs == pk.RestartTimeMs &&
		r.TraceFileIndex == pk.TraceFileIndex && r.BufferOffset == pk.BufferOffset &&
		r.RecordIndex == pk.RecordIndex
}

// FetchCall locates one call among the discovered candidates and reads the
// requested heavy columns for it — two-phase, so no candidate file is ever
// materialized whole (02 §7.5):
//
//  1. PK scan: the charged batch reader streams the pkScanRow projection and
//     remembers the match position as (row group, row offset within it).
//  2. Surgical read: the heavy columns are read at that position only, one
//     charged parquet page per column (surgicalValue).
//
// Candidates whose key-encoded pod-restart hash cannot match the PK are
// skipped without an open; a compacted object (the reserved MaintainReplica
// token) keys its hash off the compaction's inputs, so it is a candidate for
// every PK (01 §6.6, §7). The returned row carries the PK fields plus the
// surgical columns; its memory is owned by point (the caller's point lease).
// ok is false when no candidate holds the PK.
func (s *Source) FetchCall(ctx context.Context, files []FileRef, pk model.PK, point *budget.Lease, columns []string) (*storageparquet.CallV2, bool, error) {
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
		row, ok, err := s.fetchFromCandidate(ctx, ref, pk, point, columns)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return row, true, nil
		}
	}
	return nil, false, nil
}

// fetchFromCandidate runs both point-fetch phases against one candidate.
func (s *Source) fetchFromCandidate(ctx context.Context, ref FileRef, pk model.PK, point *budget.Lease, columns []string) (*storageparquet.CallV2, bool, error) {
	rgIndex, rowInRG := -1, int64(0)
	err := scanBatches(ctx, s.Store, ref, s.Budget, pkScanFootprint, s.overrun(),
		func(_ context.Context, rg int, rowOffset int64, rows []pkScanRow, _ *budget.Lease) (bool, error) {
			for i := range rows {
				if rows[i].matches(pk) {
					rgIndex, rowInRG = rg, rowOffset+int64(i)
					return true, nil
				}
			}
			return false, nil
		})
	if err != nil || rgIndex < 0 {
		return nil, false, err
	}

	row := &storageparquet.CallV2{
		Namespace: pk.PodNamespace, ServiceName: pk.PodService, PodName: pk.PodName,
		RestartTimeMs: pk.RestartTimeMs, TraceFileIndex: pk.TraceFileIndex,
		BufferOffset: pk.BufferOffset, RecordIndex: pk.RecordIndex,
	}
	found, err := s.surgicalRead(ctx, ref, rgIndex, rowInRG, columns, point, row)
	if err != nil || !found {
		return nil, false, err
	}
	return row, true, nil
}

// surgicalRead opens the candidate WITH the page index — phase 2 needs it to
// seek to the target page and to price the page before decoding (02 §7.5) —
// and reads each requested column's value for the one target row. found is
// false when the object vanished between the phases (compaction or TTL past
// the delete-grace): the same not-found semantics as a delete after the LIST,
// not an internal error.
func (s *Source) surgicalRead(ctx context.Context, ref FileRef, rgIndex int, rowInRG int64,
	columns []string, point *budget.Lease, row *storageparquet.CallV2) (found bool, err error) {

	obj, err := s.Store.Open(ctx, ref.Key)
	if errors.Is(err, ErrNotFound) {
		return false, nil // vanished between the phases
	}
	if err != nil {
		return false, errors.Wrapf(err, "open %s", ref.Key)
	}
	defer func() { _ = obj.Close() }()
	defer func() {
		if r := recover(); r != nil {
			found, err = false, errors.Errorf("read %s: %v", ref.Key, r)
		}
	}()

	f, err := parquet.OpenFile(obj, obj.Size(), parquet.SkipBloomFilters(true))
	if err != nil {
		if gone(ctx, s.Store, ref.Key) {
			return false, nil
		}
		return false, errors.Wrapf(err, "read parquet footer of %s", ref.Key)
	}
	if rgIndex >= len(f.RowGroups()) {
		return false, errors.Errorf("read %s: row group %d out of range", ref.Key, rgIndex)
	}
	for _, col := range columns {
		value, isNull, err := s.surgicalValue(ctx, f, rgIndex, rowInRG, col, point)
		if err != nil {
			if gone(ctx, s.Store, ref.Key) {
				return false, nil
			}
			return false, errors.Wrapf(err, "read %s", ref.Key)
		}
		if !isNull {
			setSurgicalColumn(row, col, value)
		}
	}
	return true, nil
}

// surgicalValue reads one column's value at (rgIndex, rowInRG): the page
// holding the row is charged before it is decoded — priced from the offset
// index and the chunk metadata, dictionary page included when the chunk has
// one — then reconciled to the decoded page's actual size. A page alone
// larger than the whole budget is an honest ErrNeverFits for the point
// endpoint (02 §7.5). A column the file predates resolves to NULL.
func (s *Source) surgicalValue(ctx context.Context, f *parquet.File, rgIndex int, rowInRG int64,
	col string, point *budget.Lease) (value []byte, isNull bool, err error) {

	leaf, ok := f.Schema().Lookup(col)
	if !ok {
		return nil, true, nil // additive column the file predates: reads as NULL
	}
	rg := f.RowGroups()[rgIndex]
	chunk := rg.ColumnChunks()[leaf.ColumnIndex]
	md := &f.Metadata().RowGroups[rgIndex].Columns[leaf.ColumnIndex].MetaData

	charge := pageCharge(chunk, md, rowInRG)
	lease, err := s.Budget.Acquire(ctx, charge)
	if err != nil {
		return nil, false, err
	}
	defer lease.Release()

	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()
	if err := pages.SeekToRow(rowInRG); err != nil {
		return nil, false, errors.Wrapf(err, "seek column %s to row %d", col, rowInRG)
	}
	page, err := pages.ReadPage()
	if err != nil {
		return nil, false, errors.Wrapf(err, "read column %s page", col)
	}
	defer parquet.Release(page)

	// Reconcile the estimate to the decoded page's actual size.
	if actual := page.Size(); actual > charge {
		if err := lease.Grow(ctx, actual-charge); err != nil {
			return nil, false, err
		}
		if hook := s.overrun(); hook != nil {
			hook()
		}
	} else {
		lease.Shrink(charge - page.Size())
	}

	// ReadPage after SeekToRow returns the page sliced to start at the target
	// row, so the first value belongs to it (flat optional column: one value
	// per row).
	var vals [1]parquet.Value
	n, err := page.Values().ReadValues(vals[:])
	if n == 0 {
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, false, errors.Wrapf(err, "decode column %s", col)
		}
		return nil, false, errors.Errorf("column %s: page holds no value for row %d", col, rowInRG)
	}
	if vals[0].IsNull() {
		return nil, true, nil
	}

	// The value points into the page buffer; grow BEFORE the copy — the page
	// and the copy coexist — then move ownership to the point lease.
	raw := vals[0].ByteArray()
	copyCost := int64(len(raw)) + containerOverhead
	if err := lease.Grow(ctx, copyCost); err != nil {
		return nil, false, err
	}
	value = append([]byte(nil), raw...)
	lease.TransferTo(point, copyCost)
	return value, false, nil
}

// pageCharge prices the page holding rowInRG before it is decoded: the
// larger of its compressed size (the offset index has it) and the chunk's
// average uncompressed page, plus one more average page when the chunk
// carries a dictionary the decode will load. Without an offset index — a
// foreign file — the whole chunk's uncompressed size is the only safe bound.
func pageCharge(chunk parquet.ColumnChunk, md *format.ColumnMetaData, rowInRG int64) int64 {
	oi, err := chunk.OffsetIndex()
	if err != nil || oi == nil || oi.NumPages() == 0 {
		return md.TotalUncompressedSize * chargeSafety
	}
	pageIdx := sort.Search(oi.NumPages(), func(i int) bool {
		return oi.FirstRowIndex(i) > rowInRG
	}) - 1
	if pageIdx < 0 {
		pageIdx = 0
	}
	avgUncompressed := (md.TotalUncompressedSize + int64(oi.NumPages()) - 1) / int64(oi.NumPages())
	charge := oi.CompressedPageSize(pageIdx)
	if avgUncompressed > charge {
		charge = avgUncompressed
	}
	if md.DictionaryPageOffset != 0 {
		charge += avgUncompressed
	}
	return charge * chargeSafety
}

// setSurgicalColumn places a surgically read value into its CallV2 field.
// The blob keeps the owned []byte; the JSON columns convert to string (a
// second, transient copy the ledger does not track — these columns are small
// next to the blob).
func setSurgicalColumn(row *storageparquet.CallV2, col string, value []byte) {
	switch col {
	case "trace_blob":
		row.TraceBlob = value
	case "truncated_reason":
		s := string(value)
		row.TruncatedReason = &s
	case "big_params_json":
		s := string(value)
		row.BigParamsJson = &s
	case "dict_words_json":
		s := string(value)
		row.DictWordsJson = &s
	case "suspend_json":
		s := string(value)
		row.SuspendJson = &s
	}
}

// overrun returns the estimate-overrun hook (nil-safe on a nil Source field).
func (s *Source) overrun() func() {
	return s.OverrunHook
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
