package cold

import (
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/parquet-go/parquet-go/format"
)

// The read-budget charge model of 02-read-contract.md §7.5. Charges are in
// bytes the reader is about to materialize; the same per-row accounting is
// used for the pre-read estimate and the post-read reconcile, so a transfer
// into the request page lease is always covered by what was charged.
const (
	// containerOverhead is the shared per-row allocation allowance
	// (model.RowOverheadBytes), aliased for the local charge formulas.
	containerOverhead = model.RowOverheadBytes

	// targetBatchBytes sizes a scan batch: K = targetBatchBytes / perRowEst,
	// clamped to [kMinRows, kMaxRows], so fat rows shrink the batch instead of
	// inflating its charge past the budget.
	targetBatchBytes = 1 << 20
	kMinRows         = 16
	kMaxRows         = 1024

	// chargeSafety multiplies every pre-read estimate: decoded Go values cost
	// more than their column bytes (headers, alignment, dictionary values
	// expanded per row). The reconcile after the read trues the charge up or
	// down, so the factor only has to cover the window between charge and
	// reconcile.
	chargeSafety = 2
)

// perRowEstimate derives the §7.5 per-row charge for one row group from the
// footer alone: the uncompressed bytes of the charged columns averaged over
// the rows, floored by the Go-struct cost. The floor is what keeps
// dictionary-encoded short strings honest — parquet stores each distinct
// value once, but the reader still builds NumRows structs.
func perRowEstimate(rg *format.RowGroup, cols map[string]bool, structSize int64) int64 {
	var sum int64
	for i := range rg.Columns {
		md := &rg.Columns[i].MetaData
		if len(md.PathInSchema) > 0 && cols[md.PathInSchema[0]] {
			sum += md.TotalUncompressedSize
		}
	}
	per := int64(0)
	if rg.NumRows > 0 {
		per = (sum + rg.NumRows - 1) / rg.NumRows
	}
	if lower := structSize + containerOverhead; per < lower {
		per = lower
	}
	return per
}

// batchRows picks the batch size for a row group: aim at targetBatchBytes of
// charge, never fewer than kMinRows (progress) nor more than kMaxRows (cap
// on the per-batch working set).
func batchRows(perRow int64) int {
	k := targetBatchBytes / perRow
	if k < kMinRows {
		return kMinRows
	}
	if k > kMaxRows {
		return kMaxRows
	}
	return int(k)
}

// callRowFootprint is the shared accounting size of one materialized list
// row (model.RowFootprint): used for the batch reconcile, the survivor
// transfer into the page lease, and the accumulator bookkeeping — one model,
// so the ledger stays consistent.
func callRowFootprint(r *model.CallRow) int64 { return model.RowFootprint(r) }

func footprintSum(rows []model.CallRow) int64 {
	var n int64
	for i := range rows {
		n += callRowFootprint(&rows[i])
	}
	return n
}

// copyCallRow detaches a survivor from the decoder's backing arrays. The copy
// is unconditional: parquet-go does not guarantee that row values own their
// memory independently of the batch, and the survivors outlive the batch
// lease (02 §7.5).
func copyCallRow(r *model.CallRow) model.CallRow {
	out := *r
	out.PK.PodNamespace = strings.Clone(r.PK.PodNamespace)
	out.PK.PodService = strings.Clone(r.PK.PodService)
	out.PK.PodName = strings.Clone(r.PK.PodName)
	out.Method = strings.Clone(r.Method)
	out.ThreadName = strings.Clone(r.ThreadName)
	out.RetentionClass = strings.Clone(r.RetentionClass)
	out.TruncatedReason = strings.Clone(r.TruncatedReason)
	if r.Params != nil {
		out.Params = make(map[string][]string, len(r.Params))
		for k, vs := range r.Params {
			cp := make([]string, len(vs))
			for i, v := range vs {
				cp[i] = strings.Clone(v)
			}
			out.Params[strings.Clone(k)] = cp
		}
	}
	return out
}
