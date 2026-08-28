package query

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

type (
	// replicaState is one discovered collector replica within one request's
	// fan-out: its health verdict and the hot-window oldest the cold cutoff
	// derives from (02 §4.3).
	replicaState struct {
		baseURL  string
		oldestMs int64
		healthy  bool
	}

	// hotTier is the resolved hot side of one request. The fan-out is
	// re-resolved and re-issued whole on every page — no per-source state
	// survives a request (02 §2.3.1, §7.1).
	hotTier struct {
		configured     bool // a discovery is wired at all
		resolveFailed  bool // DNS/endpoint resolution itself failed
		replicas       []replicaState
		partialReasons []string
	}
)

// resolveHotTier discovers the replicas and probes their hot windows in
// parallel (02 §7.1-§7.2). A failed probe degrades to a partial result, never
// to a failed query (§7.4).
func (s *Service) resolveHotTier(ctx context.Context) hotTier {
	if s.discovery == nil {
		return hotTier{}
	}
	tier := hotTier{configured: true}
	urls, err := s.discovery.Replicas(ctx)
	if err != nil {
		tier.resolveFailed = true
		tier.partialReasons = append(tier.partialReasons, fmt.Sprintf("collector discovery: %v", err))
		return tier
	}
	tier.replicas = make([]replicaState, len(urls))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, baseURL := range urls {
		tier.replicas[i] = replicaState{baseURL: baseURL}
		wg.Add(1)
		go func(i int, baseURL string) {
			defer wg.Done()
			start := time.Now()
			w, err := s.hot.HotWindow(ctx, baseURL)
			s.metrics.observeFanout(start, err)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				tier.partialReasons = append(tier.partialReasons,
					fmt.Sprintf("collector %s hot-window: %v", baseURL, err))
				return
			}
			tier.replicas[i].oldestMs = w.OldestMs
			tier.replicas[i].healthy = true
		}(i, baseURL)
	}
	wg.Wait()
	return tier
}

// coldToMs bounds the cold LIST per §4.3, with the cutoff derived dynamically
// from the replicas' hot-window reports: cold has to reach up to the youngest
// hot window's start (max over replicas of hot_window_oldest_ms) plus the
// overlap margin, so a call that ages out of any replica right now is still
// covered by one tier (zero-gap). Any degraded hot state — no discovery, no
// replicas, a failed resolution or health probe — falls back to the full
// window: the guarantee must never depend on an unreachable replica.
func (t hotTier) coldToMs(q model.CallsQuery, overlapMs int64) int64 {
	if !t.configured {
		return q.ToMs
	}
	if t.resolveFailed || len(t.replicas) == 0 {
		return q.ToMs
	}
	maxOldest := int64(0)
	for _, r := range t.replicas {
		if !r.healthy {
			return q.ToMs
		}
		if r.oldestMs > maxOldest {
			maxOldest = r.oldestMs
		}
	}
	if cut := maxOldest + overlapMs; cut < q.ToMs {
		return cut
	}
	return q.ToMs
}

// hotCalls fans /internal/v1/calls out to every healthy replica in parallel:
// each is asked for [max(from, its hot_window_oldest), to] past the cursor
// position (02 §4.3, §2.3.1) and returns one already-sorted run for the
// merge. Each run's budget lease is folded into the request page lease under
// the collection lock — a Lease is not safe for concurrent use (02 §7.5).
// succeeded/failed feed the §8 all-sources-failed verdict; a replica whose
// hot window misses the query range entirely counts as neither. A budget
// denial comes back as budgetDenied and fails the whole request with 503
// rather than degrading to a partial result.
func (s *Service) hotCalls(ctx context.Context, tier hotTier, q model.CallsQuery,
	after *model.Position, limit int, page *budget.Lease) (runs [][]model.CallRow, succeeded, failed int, reasons []string, budgetDenied error) {

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, r := range tier.replicas {
		if !r.healthy {
			failed++
			continue
		}
		rq := q
		if r.oldestMs > rq.FromMs {
			rq.FromMs = r.oldestMs
		}
		if rq.FromMs >= rq.ToMs {
			continue // the replica's window misses the query range
		}
		wg.Add(1)
		go func(baseURL string, rq model.CallsQuery) {
			defer wg.Done()
			start := time.Now()
			rows, lease, err := s.hot.Calls(ctx, baseURL, rq, after, limit)
			s.metrics.observeFanout(start, err)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if cold.IsBudgetDenial(err) {
					budgetDenied = err
					return
				}
				failed++
				reasons = append(reasons, fmt.Sprintf("collector %s calls: %v", baseURL, err))
				return
			}
			succeeded++
			lease.TransferTo(page, lease.Held())
			lease.Release()
			if len(rows) > 0 {
				runs = append(runs, rows)
			}
		}(r.baseURL, rq)
	}
	wg.Wait()
	return runs, succeeded, failed, reasons, budgetDenied
}

// hotPods fans /internal/v1/pods out to every discovered replica. /pods needs
// no cutoff, so the health probe is skipped and the replicas are asked
// directly (02 §2.7).
func (s *Service) hotPods(ctx context.Context, fromMs, toMs int64) (pods []model.PodEntry, succeeded, failed int, reasons []string) {
	if s.discovery == nil {
		return nil, 0, 0, nil
	}
	urls, err := s.discovery.Replicas(ctx)
	if err != nil {
		return nil, 0, 1, []string{fmt.Sprintf("collector discovery: %v", err)}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, baseURL := range urls {
		wg.Add(1)
		go func(baseURL string) {
			defer wg.Done()
			start := time.Now()
			entries, err := s.hot.Pods(ctx, baseURL, fromMs, toMs)
			s.metrics.observeFanout(start, err)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed++
				reasons = append(reasons, fmt.Sprintf("collector %s pods: %v", baseURL, err))
				return
			}
			succeeded++
			pods = append(pods, entries...)
		}(baseURL)
	}
	wg.Wait()
	return pods, succeeded, failed, reasons
}
