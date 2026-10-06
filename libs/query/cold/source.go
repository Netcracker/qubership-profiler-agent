package cold

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/pkg/errors"
)

// Source is the cold tier of the /calls and /pods fan-out: discovery over
// S3 LIST, budgeted batch scans over projected parquet, and a per-source
// merge that hands the query layer one sorted, deduplicated run (02 §4.1,
// §6, §7.5). The hot sources of the Stage 1 fan-out slice plug in beside it.
type Source struct {
	Store ObjectStore
	// ListConcurrency caps parallel LISTs (PROFILER_S3_LIST_CONCURRENCY,
	// 02 §5.2). Zero falls back to the contract default of 16.
	ListConcurrency int
	// DurationThresholds mirror the collector's PROFILER_DURATION_THRESHOLDS
	// so the class pruning derives its bounds from the same tier table the
	// seal pass classified with (№10). Nil selects the table defaults.
	DurationThresholds []time.Duration
	// Budget is the process-wide read memory budget every scan draws from
	// (02 §7.5). Nil disables budgeting (tests).
	Budget *budget.Budget
	// OverrunHook fires when a post-read reconcile finds the actual batch or
	// page footprint above its pre-read charge — the estimate-honesty signal
	// of §7.5. Nil disables it.
	OverrunHook func()
}

func (s *Source) listConcurrency() int {
	if s.ListConcurrency > 0 {
		return s.ListConcurrency
	}
	return 16
}

// ScanResult is one cold read: rows in (ts_ms DESC, pk ASC) order, already
// PK-deduplicated, plus the §7.4 partial markers.
type ScanResult struct {
	Rows           []model.CallRow
	More           bool // rows remained past the limit
	PartialReasons []string
}

// Calls scans the discovered files past the cursor position into one
// deduplicated run of at most limit rows. The scan is budgeted end to end
// (02 §7.5): every batch is charged before it is decoded, survivors are
// deep-copied and moved into the caller's page lease, and the retained
// accumulator never exceeds the page limit — the merge runs inside each
// batch's charge, not after the whole scan. A budget denial aborts the scan
// with the budget error (the caller answers 503 for the whole request);
// per-file read failures stay partial reasons (02 §7.4). Discovery runs
// separately (Discover) because its LIST result also feeds the wide-query
// guard before any file is opened (§2.3.2).
func (s *Source) Calls(ctx context.Context, d Discovery, q model.CallsQuery, after *model.Position, limit int, page *budget.Lease) (ScanResult, error) {
	var res ScanResult
	ls := listScan{src: s, q: q, after: after, limit: limit, page: page}
	for _, ref := range d.Files {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		err := scanBatches(ctx, s.Store, ref, s.Budget, projectedFootprint, s.overrun(), ls.consume)
		if err != nil {
			if IsBudgetDenial(err) || ctx.Err() != nil {
				return res, err
			}
			// Bounded partial data beats failing the query (02 §7.4).
			res.PartialReasons = append(res.PartialReasons, fmt.Sprintf("s3 read %s: %v", ref.Key, err))
			continue
		}
	}
	res.Rows, res.More = ls.acc, ls.more
	return res, nil
}

// IsBudgetDenial reports whether err is a read-budget denial — the one scan
// error class that must fail the whole request (503) instead of degrading to
// a partial result (02 §7.5).
func IsBudgetDenial(err error) bool {
	return errors.Is(err, budget.ErrExhausted) || errors.Is(err, budget.ErrNeverFits)
}

// listScan is the state one budgeted /calls cold scan threads through its
// batch consumer: the capped accumulator, its accounted footprint inside the
// request page lease, and the more flag.
type listScan struct {
	src      *Source
	q        model.CallsQuery
	after    *model.Position
	limit    int
	page     *budget.Lease
	acc      []model.CallRow
	accBytes int64
	more     bool
}

// consume folds one decoded batch into the capped accumulator while the
// batch lease is still held (02 §7.5): filter, truncate the sorted run to
// the page limit, reserve the copies on top of the batch, deep-copy, move
// their footprint into the page lease, merge, and return the bytes the merge
// evicted. Rows within a batch keep the file's (ts_ms DESC, pk ASC) order,
// so each batch is a sorted run and the incremental capped merge is
// equivalent to one big MergeRuns over every run.
func (ls *listScan) consume(ctx context.Context, _ int, _ int64, rows []storageparquet.CallV2Projected, lease *budget.Lease) (bool, error) {
	run := make([]model.CallRow, 0, len(rows))
	for i := range rows {
		row := toCallRow(&rows[i])
		if !ls.q.Match(row) {
			continue
		}
		if ls.after != nil && !row.Position().After(*ls.after) {
			continue
		}
		run = append(run, row)
	}
	if len(run) > ls.limit {
		// Safe pre-merge cut: the run is sorted, so a row past the first
		// `limit` has `limit` predecessors in the total order and can never
		// make the page.
		run = run[:ls.limit]
		ls.more = true
	}
	if len(run) == 0 {
		return false, nil
	}

	// Reserve the copies BEFORE making them — the batch and the copies
	// coexist until the merge below settles what survives. The footprint of a
	// copy equals the footprint of its source row, so the reservation is
	// computable up front.
	copyBytes := footprintSum(run)
	if err := lease.Grow(ctx, copyBytes); err != nil {
		return false, err
	}
	copies := make([]model.CallRow, len(run))
	for i := range run {
		copies[i] = copyCallRow(&run[i])
	}
	lease.TransferTo(ls.page, copyBytes)

	merged, dropped := model.MergeRuns([][]model.CallRow{ls.acc, copies}, ls.limit)
	ls.more = ls.more || dropped
	mergedBytes := footprintSum(merged)
	ls.page.Shrink(ls.accBytes + copyBytes - mergedBytes)
	ls.acc, ls.accBytes = merged, mergedBytes
	return false, nil
}

// ErrAllSourcesFailed marks a discovery whose every LIST failed: with no
// other source wired, the caller maps it to 504 (02 §8).
var ErrAllSourcesFailed = errors.New("every S3 LIST prefix failed")
