package cold

import (
	"context"
	"fmt"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

// DropCompactedInputs removes the overlap a compaction leaves behind until
// its delete grace has passed (01-write-contract.md §6.6): it reads the
// footer of every discovered compacted object and drops each discovered
// object that a footer lists as an input, a compacted one included. /stats
// has no PK dedup, so without this a call in both an input and the output
// counts twice (02 §2.9).
//
// A compacted object whose footer cannot be read is dropped itself, with a
// partial reason, and its inputs are kept: reading the inputs while they last
// counts each call once, while reading both would count it twice.
func (s *Source) DropCompactedInputs(ctx context.Context, files []FileRef) (kept []FileRef, partialReasons []string, err error) {
	inputs := map[string]bool{}
	unreadable := map[string]bool{}
	for _, ref := range files {
		if ref.Replica != MaintainReplica {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		keys, err := readCompactionInputs(ctx, s.Store, ref)
		if err != nil {
			unreadable[ref.Key] = true
			partialReasons = append(partialReasons, fmt.Sprintf("s3 read %s: %v", ref.Key, err))
			continue
		}
		for _, k := range keys {
			if k != ref.Key {
				inputs[k] = true
			}
		}
	}
	kept = make([]FileRef, 0, len(files))
	for _, ref := range files {
		if !inputs[ref.Key] && !unreadable[ref.Key] {
			kept = append(kept, ref)
		}
	}
	return kept, partialReasons, nil
}

// readCompactionInputs reads a compacted object's input list from its footer
// metadata: one ranged footer read, no row data. A deleted object lists
// nothing (02 §5.1).
func readCompactionInputs(ctx context.Context, store ObjectStore, ref FileRef) (keys []string, err error) {
	obj, err := store.Open(ctx, ref.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", ref.Key)
	}
	defer func() { _ = obj.Close() }()
	defer func() {
		if r := recover(); r != nil {
			keys, err = nil, errors.Errorf("read footer of %s: %v", ref.Key, r)
		}
	}()
	f, err := parquet.OpenFile(obj, obj.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		if gone(ctx, store, ref.Key) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "read parquet footer of %s", ref.Key)
	}
	joined, ok := f.Lookup(storageparquet.CompactionInputsKey)
	if !ok || joined == "" {
		return nil, nil
	}
	return strings.Split(joined, "\n"), nil
}

// EachRow hands fn every row of the files that is in the query's window and
// meets its conditions. The scan is charged to the read memory budget batch
// by batch (02 §7.5); the row passed to fn is valid only during the call, so
// fn copies what it keeps. A budget denial or an error from fn ends the scan
// with that error; a file that cannot be read becomes a partial reason
// (02 §7.4).
func (s *Source) EachRow(ctx context.Context, files []FileRef, q model.CallsQuery, fn func(*model.CallRow) error) (partialReasons []string, err error) {
	matcher, err := q.Matcher()
	if err != nil {
		return nil, err
	}
	var fnErr error
	consume := func(_ context.Context, _ int, _ int64, rows []storageparquet.CallV2Projected, _ *budget.Lease) (bool, error) {
		for i := range rows {
			row := toCallRow(&rows[i])
			if !matcher.Match(&row) {
				continue
			}
			if err := fn(&row); err != nil {
				fnErr = err
				return false, err
			}
		}
		return false, nil
	}
	for _, ref := range files {
		if err := ctx.Err(); err != nil {
			return partialReasons, err
		}
		err := scanBatches(ctx, s.Store, ref, s.Budget, projectedFootprint, s.overrun(), consume)
		if err == nil {
			continue
		}
		if fnErr != nil || IsBudgetDenial(err) || ctx.Err() != nil {
			return partialReasons, err
		}
		partialReasons = append(partialReasons, fmt.Sprintf("s3 read %s: %v", ref.Key, err))
	}
	return partialReasons, nil
}
