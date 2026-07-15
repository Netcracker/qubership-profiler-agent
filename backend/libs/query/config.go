// Package query composes the external read API of 02-read-contract.md: the
// /api/v1 HTTP surface, keyset pagination cursors (§2.3.1), the wide-query
// guard (§2.3.2), the hot collector fan-out (§3, §4, §7), and the tier merge
// with cold-preferred PK dedup (§6). The cold tier reads S3; the hot tier
// fans out to the collector replicas discovered through the headless Service.
package query

import "time"

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
	// MaxScanFiles / MaxScanBytes are the §2.3.2 cost layer
	// (PROFILER_MAX_SCAN_FILES / PROFILER_MAX_SCAN_BYTES).
	MaxScanFiles int
	MaxScanBytes int64
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
	if c.MaxScanFiles <= 0 {
		c.MaxScanFiles = 10_000
	}
	if c.MaxScanBytes <= 0 {
		c.MaxScanBytes = 2 << 30 // 2 GB
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
	return c
}
