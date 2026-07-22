// Package query composes the external read API of 02-read-contract.md: the
// /api/v1 HTTP surface, keyset pagination cursors (§2.3.1), the wide-query
// guard (§2.3.2), the hot collector fan-out (§3, §4, §7), and the tier merge
// with cold-preferred PK dedup (§6). The cold tier reads S3; the hot tier
// fans out to the collector replicas discovered through the headless Service.
package query

import (
	"net/url"
	"time"
)

// Config carries the query-service knobs of 02-read-contract.md §9. Zero
// values fall back to the contract defaults via Normalize; env parsing
// (PROFILER_*) belongs to the app wiring task, mirroring hotstore.Config.
type Config struct {
	// ListenAddr binds /api/v1 (PROFILER_EXTERNAL_API_PORT, default :8080).
	ListenAddr string
	// CursorTTL bounds a pagination cursor's validity (PROFILER_CURSOR_TTL).
	CursorTTL time.Duration
	// WideRangeLimit is the §2.3.2 span layer (PROFILER_WIDE_RANGE_LIMIT).
	WideRangeLimit time.Duration
	// PodsRangeLimit bounds the /pods window (PROFILER_MAX_PODS_RANGE): /pods
	// has no file-pruning filter and lists one S3 prefix per UTC day, so it
	// needs its own, more generous span guard than /calls (PR 708 review #3).
	PodsRangeLimit time.Duration
	// MaxScanFiles / MaxScanBytes are the §2.3.2 cost layer
	// (PROFILER_MAX_SCAN_FILES / PROFILER_MAX_SCAN_BYTES).
	MaxScanFiles int
	MaxScanBytes int64
	// ReadMemoryBudget is the process-wide read memory budget every reader
	// draws from (PROFILER_READ_MEMORY_BUDGET, §7.5).
	ReadMemoryBudget int64
	// ReadBudgetWait bounds the queue wait on a budget charge
	// (PROFILER_READ_BUDGET_WAIT, §7.5); past it the request answers 503.
	ReadBudgetWait time.Duration
	// DurationThresholds mirror the collector's PROFILER_DURATION_THRESHOLDS:
	// the class pruning and the guard exemption derive their bounds from the
	// same tier table the seal pass classified with (№10). Nil selects the
	// model.RetentionTiers defaults.
	DurationThresholds []time.Duration
	// ListConcurrency caps parallel S3 LISTs (PROFILER_S3_LIST_CONCURRENCY).
	ListConcurrency int
	// DefaultLimit / MaxLimit bound the /calls page size (02 §2.3).
	DefaultLimit int
	MaxLimit     int
	// CollectorService enables the hot tier: the headless-Service name the
	// fan-out re-resolves on every request (COLLECTOR_HEADLESS_SVC, §7.1).
	// Empty leaves the query cold-only unless Options wires a Discovery.
	CollectorService string
	// CollectorPort is the replicas' internal API port
	// (PROFILER_INTERNAL_API_PORT on the collector side, 02 §9).
	CollectorPort int
	// FanoutTimeout bounds each per-replica read (PROFILER_FANOUT_TIMEOUT, §7.2).
	FanoutTimeout time.Duration
	// OverlapMargin sizes the hot/cold overlap window the dynamic cutoff adds
	// on top of the replicas' hot-window reports (PROFILER_OVERLAP_MARGIN, §4.3).
	OverlapMargin time.Duration
	// DumpsCollectorURL is the dumps-collector base URL (DUMPS_COLLECTOR_URL),
	// e.g. "https://dumps-collector-<namespace>.<cloud-public-host>". It is a
	// separate deployment with its own ingress, so there is no in-cluster way
	// to derive it; empty (the default) leaves the Pods Info dump link-out
	// unavailable rather than guess a scheme and host that may not resolve
	// (PR 708 review #18).
	DumpsCollectorURL string
}

// Normalize fills unset fields with the 02 §9 defaults.
func (c Config) Normalize() Config {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.CursorTTL <= 0 {
		c.CursorTTL = 15 * time.Minute
	}
	if c.WideRangeLimit <= 0 {
		c.WideRangeLimit = 6 * time.Hour
	}
	if c.PodsRangeLimit <= 0 {
		c.PodsRangeLimit = 366 * 24 * time.Hour
	}
	if c.MaxScanFiles <= 0 {
		c.MaxScanFiles = 10_000
	}
	if c.MaxScanBytes <= 0 {
		c.MaxScanBytes = 2 << 30 // 2 GB
	}
	if c.ReadMemoryBudget <= 0 {
		c.ReadMemoryBudget = 512 << 20 // 512 MB, sized for the default 2 Gi pod (02 §7.5)
	}
	if c.ReadBudgetWait <= 0 {
		c.ReadBudgetWait = 5 * time.Second
	}
	if c.ListConcurrency <= 0 {
		c.ListConcurrency = 16
	}
	if c.DefaultLimit <= 0 {
		c.DefaultLimit = 100
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 1000
	}
	if c.CollectorPort <= 0 {
		c.CollectorPort = 8081
	}
	if c.FanoutTimeout <= 0 {
		c.FanoutTimeout = 2 * time.Second
	}
	if c.OverlapMargin <= 0 {
		c.OverlapMargin = 5 * time.Minute
	}
	c.DumpsCollectorURL = safeDumpsCollectorURL(c.DumpsCollectorURL)
	return c
}

// safeDumpsCollectorURL keeps the dumps-collector base only when it is an
// absolute http(s) URL. Anything else — a javascript: scheme, a relative
// value, junk — disables the Pods Info link-out rather than letting /config
// echo a value the UI would turn into a clickable href (PR 708 review #10).
func safeDumpsCollectorURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ""
	}
	return raw
}
