package cold

import (
	"context"
	"fmt"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Source is the cold tier of the /calls and /pods fan-out: discovery over
// S3 LIST, projected parquet scans, and a per-source merge that hands the
// query layer one sorted, deduplicated run (02 §4.1, §6). The hot sources of
// the Stage 1 fan-out slice plug in beside it.
type Source struct {
	Store ObjectStore
	// ListConcurrency caps parallel LISTs (PROFILER_S3_LIST_CONCURRENCY,
	// 02 §5.2). Zero falls back to the contract default of 16.
	ListConcurrency int
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

// Calls scans the discovered files, seeks past the cursor position, and
// merges the per-file runs into one deduplicated run of at most limit rows
// (02 §2.3.1: each source seeks past the cursor and returns up to limit
// rows). Discovery runs separately (Discover) because its LIST result also
// feeds the wide-query guard before any file is opened (§2.3.2).
func (s *Source) Calls(ctx context.Context, d Discovery, q model.CallsQuery, after *model.Position, limit int) (ScanResult, error) {
	var res ScanResult
	runs := make([][]model.CallRow, 0, len(d.Files))
	for _, ref := range d.Files {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		rows, err := ScanFile(ctx, s.Store, ref, q, after)
		if err != nil {
			// Bounded partial data beats failing the query (02 §7.4).
			res.PartialReasons = append(res.PartialReasons, fmt.Sprintf("s3 read %s: %v", ref.Key, err))
			continue
		}
		if len(rows) > 0 {
			runs = append(runs, rows)
		}
	}
	res.Rows, res.More = model.MergeRuns(runs, limit)
	return res, nil
}

// ErrAllSourcesFailed marks a discovery whose every LIST failed: with no
// other source wired, the caller maps it to 504 (02 §8).
var ErrAllSourcesFailed = errors.New("every S3 LIST prefix failed")
