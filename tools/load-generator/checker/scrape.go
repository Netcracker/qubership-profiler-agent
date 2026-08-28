package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// metrics is one target's scrape, flattened to "name" or "name{k=v,...}" with
// label pairs sorted, so a series key is stable across scrapes.
type metrics map[string]float64

// gapTracker counts consecutive failed polls per source target. A silent
// source must not pass by absence (doc/checker.md): past the configured gap
// the target-available invariant latches a violation.
type gapTracker struct {
	warmup  time.Duration
	started time.Time
	gaps    map[string]int
	lastAt  map[string]time.Time // time of the newest failed poll per target
}

func newGapTracker(warmup time.Duration) *gapTracker {
	return &gapTracker{warmup: warmup, started: time.Now(),
		gaps: map[string]int{}, lastAt: map[string]time.Time{}}
}

func (g *gapTracker) observe(target string, ok bool) {
	if ok {
		g.gaps[target] = 0
		return
	}
	g.gaps[target]++
	g.lastAt[target] = time.Now()
}

// findings judges the gaps. A gap is expected only when the target is mapped
// to a pod (targetPods) whose scrape-gap allowance window covers the newest
// failed poll — unmapped targets and other pods stay violations
// (doc/checker.md, "Expected failures").
func (g *gapTracker) findings(maxGap int, faults *faultState, targetPods map[string]string) []finding {
	if time.Since(g.started) < g.warmup {
		return nil
	}
	var out []finding
	for target, gap := range g.gaps {
		if gap <= maxGap {
			continue
		}
		at := g.lastAt[target]
		expected := false
		if faults != nil {
			if pod, ok := targetPods[target]; ok {
				expected = faults.expectedForPod("scrape-gap", pod, at)
			}
			// The query API and the S3 listing have no pod identity to match:
			// query goes partially dark while a collector is down, and the
			// listing dies with a scaled-down MinIO. Scope both to any
			// scrape-gap window.
			if target == "query-api" || target == "s3" {
				expected = faults.expected("scrape-gap", at)
			}
		}
		out = append(out, finding{subject: target, observedAt: at, expected: expected,
			msg: fmt.Sprintf("no data for %d consecutive polls (max %d)", gap, maxGap)})
	}
	sortFindings(out)
	return out
}

// sample is one poll of every target at one instant.
type sample struct {
	at      time.Time
	targets map[string]metrics
}

// scrapeAll polls every target once. A failed target is reported and simply
// absent from the sample — a soak must survive a collector restart without
// the checker dying, and §8.8 (no unexplained restarts) is a separate check.
func scrapeAll(ctx context.Context, targets []string) (sample, []error) {
	s := sample{at: time.Now(), targets: make(map[string]metrics, len(targets))}
	var errs []error
	for _, target := range targets {
		target = strings.TrimSpace(target)
		m, err := scrape(ctx, target)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", target, err))
			continue
		}
		s.targets[target] = m
	}
	return s, errs
}

func scrape(ctx context.Context, url string) (metrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}

	// The zero-value TextParser panics on an unset validation scheme since
	// prometheus/common v0.67; the constructor is mandatory.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}
	out := make(metrics, len(families))
	for name, mf := range families {
		for _, m := range mf.GetMetric() {
			key := name
			if len(m.GetLabel()) > 0 {
				pairs := make([]string, 0, len(m.GetLabel()))
				for _, l := range m.GetLabel() {
					pairs = append(pairs, l.GetName()+"="+l.GetValue())
				}
				sort.Strings(pairs)
				key = name + "{" + strings.Join(pairs, ",") + "}"
			}
			switch {
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			case m.GetUntyped() != nil:
				out[key] = m.GetUntyped().GetValue()
				// Histogram and summary families are skipped: no §8 invariant
				// reads one, and flattening buckets would bloat the history.
			}
		}
	}
	return out, nil
}
