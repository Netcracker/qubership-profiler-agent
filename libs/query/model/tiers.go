package model

import (
	"math"
	"time"
)

const day = 24 * time.Hour

// RetentionTier is one row of the retention tier table: the class name, the
// exclusive upper duration bound for clean calls, and the default TTL.
type RetentionTier struct {
	Class string
	// UpperMs is the exclusive upper bound (in ms) of the clean-call duration
	// range this tier holds; math.MaxInt64 marks the open-ended last clean
	// tier. Zero marks a class keyed on error_flag, not duration — such a
	// class holds calls of any duration and is never pruned by a duration
	// filter.
	UpperMs int64
	TTL     time.Duration
}

// RetentionTiers is THE tier table (00-plan.md decisions 3 and 4): the write
// classification (hotstore Config.RetentionClass), the read pruning bounds
// (cold discovery), and the per-class TTL (maintain, envconfig, chart) are
// all derived from this one ordered list — never from a second hardcode.
var RetentionTiers = []RetentionTier{
	{Class: RetentionShortClean, UpperMs: 100, TTL: 2 * day},
	{Class: RetentionNormalClean, UpperMs: 1000, TTL: 7 * day},
	{Class: RetentionLongClean, UpperMs: 10_000, TTL: 30 * day},
	{Class: RetentionHugeClean, UpperMs: math.MaxInt64, TTL: 180 * day},
	{Class: RetentionAnyError, TTL: 180 * day},
	{Class: RetentionCorrupted, TTL: 7 * day},
}

// CleanTiers returns the duration-classified tiers in ascending bound order.
func CleanTiers() []RetentionTier {
	out := make([]RetentionTier, 0, len(RetentionTiers))
	for _, t := range RetentionTiers {
		if t.UpperMs > 0 {
			out = append(out, t)
		}
	}
	return out
}

// DefaultDurationThresholds derives the classification thresholds — the
// finite clean-tier bounds — from the tier table: [100ms, 1s, 10s].
func DefaultDurationThresholds() []time.Duration {
	var out []time.Duration
	for _, t := range CleanTiers() {
		if t.UpperMs != math.MaxInt64 {
			out = append(out, time.Duration(t.UpperMs)*time.Millisecond)
		}
	}
	return out
}

// ClassifyDuration maps one call to its retention class. thresholds override
// the clean-tier bounds (PROFILER_DURATION_THRESHOLDS) and must hold exactly
// len(CleanTiers())-1 ascending values; nil selects the table defaults.
func ClassifyDuration(duration time.Duration, errorFlag bool, thresholds []time.Duration) string {
	if errorFlag {
		return RetentionAnyError
	}
	if thresholds == nil {
		thresholds = DefaultDurationThresholds()
	}
	clean := CleanTiers()
	for i, bound := range thresholds {
		if duration < bound {
			return clean[i].Class
		}
	}
	return clean[len(clean)-1].Class
}

// CleanClassUpperMs derives the read-side pruning bounds from the same
// thresholds the write side classifies with: a clean class whose whole
// duration range sits below a query's duration_min_ms holds no matching call
// (02 §2.3.2, §5.5). nil thresholds select the table defaults.
func CleanClassUpperMs(thresholds []time.Duration) map[string]int64 {
	if thresholds == nil {
		thresholds = DefaultDurationThresholds()
	}
	clean := CleanTiers()
	out := make(map[string]int64, len(clean))
	for i, t := range clean {
		if i < len(thresholds) {
			out[t.Class] = thresholds[i].Milliseconds()
		} else {
			out[t.Class] = math.MaxInt64
		}
	}
	return out
}

// DefaultClassTTL returns the per-class TTL column of the tier table.
func DefaultClassTTL() map[string]time.Duration {
	out := make(map[string]time.Duration, len(RetentionTiers))
	for _, t := range RetentionTiers {
		out[t.Class] = t.TTL
	}
	return out
}

// MaxClassTTL is the longest per-class TTL of the table; the pods-manifest
// retention derives from it (longest parquet TTL plus a safety margin), so a
// readable row never outlives the manifest that names its pod-restart.
func MaxClassTTL() time.Duration {
	var max time.Duration
	for _, t := range RetentionTiers {
		if t.TTL > max {
			max = t.TTL
		}
	}
	return max
}
