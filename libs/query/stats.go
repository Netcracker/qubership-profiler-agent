package query

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/hot"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/labstack/echo/v4"
)

// reasonReadBudgetExhausted is the partial reason of a /stats response whose
// hot hash sets did not fit the read memory budget (02 §2.9).
const reasonReadBudgetExhausted = "read_budget_exhausted"

// hotStats is the hot half of one /stats request.
type hotStats struct {
	results []hot.StatsResult
	hashes  stats.HashSet
	// tLoMs splits the window: cold alone below it, hot first and the cold
	// remainder from it on. It equals the window's to when no hot source
	// contributes.
	tLoMs             int64
	succeeded, failed int
	partialReasons    []string
	budgetDenied      error
}

// handleStats serves GET /api/v1/stats (02 §2.9). It splits the window at
// t_lo, the earliest hot window of the replicas that answered the health
// probe: below it the cold tier holds every call and aggregates alone; from
// it on each replica aggregates its own calls and returns their PK hashes,
// and the cold scan counts only the calls no replica counted. Nothing is
// deduplicated row by row, so the response holds no cursor and no row
// merge.
func (s *Service) handleStats(c echo.Context) error {
	ctx := c.Request().Context()
	req, detail := stats.ParseRequest(c.QueryParams())
	if detail != "" {
		return badRequest(c, detail)
	}
	q := req.Query
	// An aggregate reads every file of the window, so the guard runs with no
	// order exemption and charges the full estimate (02 §2.9).
	if rej := s.guardSpan(q, s.cfg.WideRangeLimit, s.cfg.PodsRangeLimit); rej != nil {
		return s.sendGuardRejection(c, rej)
	}

	lease := s.budget.NewLease()
	defer lease.Release()
	charge := func(n int64) error { return lease.Grow(ctx, n) }

	h := s.hotStats(ctx, req, lease)
	partial := append([]string{}, h.partialReasons...)
	succeeded, failed := h.succeeded, h.failed

	discovery, err := s.cold.Discover(ctx, q)
	if err != nil {
		return err
	}
	if rej := guardCost(q, discovery.Files, s.cfg.MaxScanFiles, s.cfg.MaxScanBytes); rej != nil {
		return s.sendGuardRejection(c, rej)
	}
	files, lineageReasons, err := s.cold.DropCompactedInputs(ctx, discovery.Files)
	if err != nil {
		return err
	}
	partial = append(partial, discovery.PartialReasons...)
	partial = append(partial, lineageReasons...)

	coldAgg := stats.NewAggregator(req.Grouping(), req.Order, s.cfg.StatsMaxGroups, charge)
	scanReasons, err := s.cold.EachRow(ctx, files, q, func(row *model.CallRow) error {
		if row.TsMs >= h.tLoMs && len(h.hashes) > 0 && h.hashes.Contains(stats.PKHash(row.PK)) {
			return nil // a replica counted this call (02 §2.9)
		}
		return coldAgg.Add(row)
	})
	if err != nil {
		if cold.IsBudgetDenial(err) {
			return s.sendBudgetRejection(c, budgetEndpointStats, err, &q, &discovery)
		}
		return err
	}
	partial = append(partial, scanReasons...)
	if discovery.Prefixes > 0 && discovery.FailedPrefixes == discovery.Prefixes {
		failed++
	} else {
		succeeded++
	}
	// §8: 504 only when every attempted source failed.
	if succeeded == 0 && failed > 0 {
		return gatewayTimeout(c, partial)
	}

	merger := stats.NewMerger(charge)
	if err := merger.AddSource(coldAgg.Groups(), coldAgg.Totals(), coldAgg.Approximate()); err != nil {
		return s.sendBudgetRejection(c, budgetEndpointStats, err, &q, &discovery)
	}
	for _, r := range h.results {
		if err := merger.AddSource(r.Groups, r.Totals, r.Approximate); err != nil {
			return s.sendBudgetRejection(c, budgetEndpointStats, err, &q, &discovery)
		}
	}
	resp := merger.Response(req)
	resp.Partial = len(partial) > 0
	if resp.Partial {
		resp.PartialReasons = partial
	}
	s.metrics.countPartial(resp.Partial)
	return c.JSON(http.StatusOK, resp)
}

// hotStats resolves the hot tier and fans /internal/v1/stats out to the
// replicas for [t_lo, to). A tier that is degraded as a whole (02 §2.9:
// discovery failed, no replica, or no replica answered its probe) adds no
// source and marks the response partial; so does a hash set that does not fit
// the read memory budget, which drops every hot result. A single replica
// that fails is left out with a partial reason.
func (s *Service) hotStats(ctx context.Context, req stats.Request, lease *budget.Lease) hotStats {
	q := req.Query
	h := hotStats{tLoMs: q.ToMs}
	tier := s.resolveHotTier(ctx)
	if !tier.configured {
		return h // a cold-only deployment: nothing is missing
	}
	h.partialReasons = append(h.partialReasons, tier.partialReasons...)
	var healthy []replicaState
	for _, r := range tier.replicas {
		if r.healthy {
			healthy = append(healthy, r)
		} else {
			h.failed++
		}
	}
	if tier.resolveFailed {
		h.failed++
	}
	if len(healthy) == 0 {
		if !tier.resolveFailed && len(tier.replicas) == 0 {
			h.partialReasons = append(h.partialReasons, "collector discovery: no replica found")
		}
		return h
	}
	tLo := healthy[0].oldestMs
	for _, r := range healthy[1:] {
		tLo = min(tLo, r.oldestMs)
	}
	h.tLoMs = max(q.FromMs, min(tLo, q.ToMs))
	if h.tLoMs >= q.ToMs {
		return h // the window ends before every hot window: cold only
	}

	params := req.InternalValues(h.tLoMs, s.cfg.StatsMaxGroups)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var leases []*budget.Lease
	for _, r := range healthy {
		if r.oldestMs >= q.ToMs {
			continue // this replica holds nothing in the window
		}
		wg.Add(1)
		go func(baseURL string) {
			defer wg.Done()
			start := time.Now()
			res, l, err := s.hot.Stats(ctx, baseURL, params)
			s.metrics.observeFanout(start, err)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if cold.IsBudgetDenial(err) {
					h.budgetDenied = err
					return
				}
				h.failed++
				h.partialReasons = append(h.partialReasons, fmt.Sprintf("collector %s stats: %v", baseURL, err))
				return
			}
			h.succeeded++
			leases = append(leases, l)
			h.results = append(h.results, res)
		}(r.baseURL)
	}
	wg.Wait()

	if h.budgetDenied == nil {
		lists := make([][]uint64, 0, len(h.results))
		for _, r := range h.results {
			lists = append(lists, r.Hashes)
		}
		// The merged set replaces the per-replica lists, so move their
		// charge into the request lease rather than charging twice.
		for _, l := range leases {
			l.TransferTo(lease, l.Held())
			l.Release()
		}
		h.hashes = stats.NewHashSet(lists...)
		for i := range h.results {
			h.results[i].Hashes = nil
		}
		return h
	}
	// The hash sets do not fit: drop the hot side and let the cold scan cover
	// the whole window, as for a degraded tier (02 §2.9).
	for _, l := range leases {
		l.Release()
	}
	return hotStats{
		tLoMs:          q.ToMs,
		partialReasons: append(h.partialReasons, reasonReadBudgetExhausted),
		failed:         h.failed,
	}
}
