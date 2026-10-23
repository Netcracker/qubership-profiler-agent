package query

import (
	"context"
	"slices"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// coldPage is the cold tier's share of one /calls page.
type coldPage struct {
	rows           []model.CallRow
	more           bool
	partialReasons []string
	// allFailed reports that every LIST prefix failed: the cold source
	// counts as failed for the §8 verdict.
	allFailed bool
	// stop is set when the page could not read every band it needed: rows
	// past it are not known to be complete (02 §2.3.5).
	stop *model.Position
	// discovery is what the page listed, for the §7.5 budget-rejection body.
	discovery *cold.Discovery
}

// coldScan reads the cold share of a page. A guard rejection comes back as
// the second result; an error is a budget denial or an internal failure.
type coldScan func(ctx context.Context, q model.CallsQuery, after *model.Position, limit int,
	page *budget.Lease) (coldPage, *guardRejection, error)

// coldCalls is the start_desc cold read: one discovery over every class the
// query allows, the cost guard over all of it, and one scan.
func (s *Service) coldCalls(ctx context.Context, q model.CallsQuery, after *model.Position, limit int,
	page *budget.Lease) (coldPage, *guardRejection, error) {

	var res coldPage
	discovery, err := s.cold.Discover(ctx, q)
	if err != nil {
		return res, nil, err
	}
	res.discovery = &discovery
	// Like the span guard, the cost guard runs on every page so a forged
	// cursor cannot skip it (PR 708 review #2). A later page's cold window
	// is bounded by the cutoff, so its file set is a subset of page 1's —
	// an honest cursor that passed page 1 passes here too.
	if rej := guardCost(q, discovery.Files, s.cfg.MaxScanFiles, s.cfg.MaxScanBytes); rej != nil {
		return res, rej, nil
	}
	scan, err := s.cold.Calls(ctx, discovery, q, after, limit, page)
	if err != nil {
		return res, nil, err
	}
	res.rows, res.more = scan.Rows, scan.More
	res.partialReasons = append(discovery.PartialReasons, scan.PartialReasons...)
	res.allFailed = discovery.Prefixes > 0 && discovery.FailedPrefixes == discovery.Prefixes
	return res, nil, nil
}

// coldCallsByDuration is the duration_desc cold read (02 §2.3.5): the clean
// bands from `band` down, each merged with the error classes, which hold
// calls of any duration and are read with the first band. The walk stops once
// `limit` rows sit at or above the lower bound of the band just read, since
// every row of a shorter band is shorter. The cost guard runs before each band
// opens, over what the page has opened so far plus that band: the first band
// over the limits rejects the page, a later one stops it at the lower bound
// of the last band read.
func (s *Service) coldCallsByDuration(bands []model.DurationBand, band string) coldScan {
	return func(ctx context.Context, q model.CallsQuery, after *model.Position, limit int,
		page *budget.Lease) (coldPage, *guardRejection, error) {

		var res coldPage
		allowed := s.cold.ClassesFor(q)
		_, maxMs := q.DurationBounds()
		var errorClasses []string
		for _, class := range allowed {
			if class == model.RetentionAnyError || class == model.RetentionCorrupted {
				errorClasses = append(errorClasses, class)
			}
		}
		// Each step opens a set of classes: the error classes with the first
		// band, then one band per step.
		type step struct {
			classes []string
			band    *model.DurationBand
		}
		var steps []step
		started := false
		for i := range bands {
			b := &bands[i]
			started = started || b.Class == band
			if !started || !slices.Contains(allowed, b.Class) || b.LowerMs > maxMs {
				continue // longer than the cursor, or excluded by a condition
			}
			steps = append(steps, step{classes: []string{b.Class}, band: b})
		}
		if len(errorClasses) > 0 {
			if len(steps) == 0 {
				steps = []step{{}}
			}
			steps[0].classes = append(errorClasses, steps[0].classes...)
		}

		ls, err := s.cold.NewListScan(q, after, limit, page)
		if err != nil {
			return res, nil, err
		}
		opened := &cold.Discovery{}
		res.discovery = opened
		lastLowerMs := int64(-1)
		for i, st := range steps {
			d, err := s.cold.DiscoverClasses(ctx, q, st.classes)
			if err != nil {
				return res, nil, err
			}
			files := append(slices.Clone(opened.Files), d.Files...)
			if rej := guardCost(q, files, s.cfg.MaxScanFiles, s.cfg.MaxScanBytes); rej != nil {
				if i == 0 {
					return res, rej, nil
				}
				// Every row at or above the last band's lower bound has been
				// read; the next page starts at the band that did not fit.
				res.stop = &model.Position{DurationMs: int32(lastLowerMs), NoPK: true}
				break
			}
			opened.Files = files
			opened.Prefixes += d.Prefixes
			opened.FailedPrefixes += d.FailedPrefixes
			opened.PartialReasons = append(opened.PartialReasons, d.PartialReasons...)
			reasons, err := ls.Scan(ctx, d.Files)
			res.partialReasons = append(res.partialReasons, reasons...)
			if err != nil {
				return res, nil, err
			}
			if st.band == nil {
				continue
			}
			lastLowerMs = st.band.LowerMs
			if rows := ls.Rows(); len(rows) == limit && int64(rows[limit-1].DurationMs) >= lastLowerMs {
				break
			}
		}
		res.rows = ls.Rows()
		// A full page may leave rows in the bands it did not open.
		res.more = ls.More() || len(res.rows) == limit
		res.partialReasons = append(opened.PartialReasons, res.partialReasons...)
		res.allFailed = opened.Prefixes > 0 && opened.FailedPrefixes == opened.Prefixes
		return res, nil, nil
	}
}

// cursorBand reads the band a duration_desc cursor resumes at. A cursor
// without one, which only a forged token can be, resumes at the band of its
// position.
func cursorBand(bands []model.DurationBand, tok cursorToken) (string, error) {
	if tok.Band == "" {
		return model.BandFor(bands, tok.Pos), nil
	}
	for _, b := range bands {
		if b.Class == tok.Band {
			return tok.Band, nil
		}
	}
	return "", errors.Errorf("cursor band %q is not a clean retention class", tok.Band)
}
