package hotread

import (
	"net/http"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/labstack/echo/v4"
)

// statsPageRows is how many index rows one SQL page of the stats walk reads:
// enough to keep the per-page subquery cheap, small enough that a request
// never holds a whole partition (№15).
var statsPageRows = 5000

// handleStats serves GET /internal/v1/stats (02 §3): the replica's own rows
// in [from, to) whose sealed copy is not yet confirmed in S3, aggregated into
// every group up to max_groups, with sketch bins instead of percentiles, and
// the PK hashes of exactly the counted calls. The query service lists S3
// only after every replica answered, so the cold scan finds each call this
// replica left out and counts it with the values seal corrected, and skips
// the cold copy of each call counted here (§2.9).
func (a *API) handleStats(c echo.Context) error {
	req, detail := stats.ParseInternalRequest(c.QueryParams())
	if detail != "" {
		return badRequest(c, detail)
	}
	// The snapshot comes first: a pass uploaded while the walk runs only
	// keeps its calls on the hot side, where their hashes cover the cold copy.
	uploaded, err := a.store.UploadedCalls()
	if err != nil {
		return err
	}
	agg := stats.NewAggregator(req.Grouping, req.Order, req.MaxGroups, nil)
	var hashes []uint64
	err = a.eachCall(req.Query, func(bucket int64, idx *hotstore.CallIndexRow, row *model.CallRow) error {
		if uploaded.Uploaded(bucket, idx.PodRestart, idx.CallsWalOffset) {
			return nil // the cold scan counts its sealed copy
		}
		hashes = append(hashes, stats.PKHash(row.PK))
		return agg.Add(row)
	})
	if err != nil {
		return err
	}
	resp := stats.InternalResponse{
		GroupBy:     req.Grouping.Field,
		Groups:      make([]stats.InternalGroup, 0),
		Totals:      agg.Totals(),
		Approximate: agg.Approximate(),
		PKHashes:    stats.EncodeHashes(hashes),
	}
	for _, g := range agg.Groups() {
		resp.Groups = append(resp.Groups, stats.InternalGroupOf(g))
	}
	return c.JSON(http.StatusOK, resp)
}

// eachCall hands fn every indexed call of the window that meets the query's
// conditions, with the same SQL pushdown and Go-side matching as /calls,
// along with its partition bucket and index row. The rows are reused after fn
// returns.
func (a *API) eachCall(q model.CallsQuery, fn func(bucket int64, idx *hotstore.CallIndexRow, row *model.CallRow) error) error {
	cfg := a.store.Config()
	buckets, err := a.store.Buckets()
	if err != nil {
		return err
	}
	matcher, err := q.Matcher()
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		start := cfg.BucketStartMs(bucket)
		if start >= q.ToMs || start+cfg.TimeBucket.Milliseconds() <= q.FromMs {
			continue
		}
		// Pages descend by ts and hold whole tie groups, so the next page
		// starts strictly below the last ts of this one (see queryCalls).
		pageToMs := q.ToMs
		for {
			idxRows, err := a.store.CallsPage(bucket, q, q.FromMs, pageToMs, statsPageRows)
			if err != nil {
				return err
			}
			for i := range idxRows {
				idx := &idxRows[i]
				row, err := a.toCallRow(*idx)
				if err != nil {
					return err
				}
				if !matcher.Match(&row) {
					continue
				}
				if err := fn(bucket, idx, &row); err != nil {
					return err
				}
			}
			if len(idxRows) < statsPageRows {
				break
			}
			pageToMs = idxRows[len(idxRows)-1].TsMs
			if pageToMs <= q.FromMs {
				break
			}
		}
	}
	return nil
}
