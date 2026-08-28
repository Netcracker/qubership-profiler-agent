package query

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
)

// TestColdToMs pins the §4.3 dynamic cutoff: the cold LIST reaches up to the
// youngest hot window's start plus the overlap margin, and any degraded hot
// state falls back to the full window so zero-gap never depends on an
// unreachable replica.
func TestColdToMs(t *testing.T) {
	const overlapMs = int64(300_000)
	q := model.CallsQuery{FromMs: 1_000_000, ToMs: 2_000_000}

	healthy := func(oldest ...int64) hotTier {
		tier := hotTier{configured: true}
		for _, o := range oldest {
			tier.replicas = append(tier.replicas, replicaState{oldestMs: o, healthy: true})
		}
		return tier
	}

	t.Run("hot tier off: cold covers the window", func(t *testing.T) {
		assert.Equal(t, q.ToMs, hotTier{}.coldToMs(q, overlapMs))
	})
	t.Run("discovery failed: full window", func(t *testing.T) {
		assert.Equal(t, q.ToMs, hotTier{configured: true, resolveFailed: true}.coldToMs(q, overlapMs))
	})
	t.Run("no replicas resolved: full window", func(t *testing.T) {
		assert.Equal(t, q.ToMs, hotTier{configured: true}.coldToMs(q, overlapMs))
	})
	t.Run("a failed health probe degrades to the full window", func(t *testing.T) {
		tier := healthy(1_200_000)
		tier.replicas = append(tier.replicas, replicaState{healthy: false})
		assert.Equal(t, q.ToMs, tier.coldToMs(q, overlapMs))
	})
	t.Run("cutoff = max(oldest) + overlap", func(t *testing.T) {
		// The replica with the YOUNGEST hot window (largest oldest) decides:
		// data below its window start exists only in cold.
		assert.Equal(t, int64(1_500_000+300_000), healthy(1_100_000, 1_500_000).coldToMs(q, overlapMs))
	})
	t.Run("cutoff clamps to the query window", func(t *testing.T) {
		assert.Equal(t, q.ToMs, healthy(1_900_000).coldToMs(q, overlapMs))
	})
	t.Run("hot windows older than the range skip cold entirely", func(t *testing.T) {
		wide := healthy(500_000)
		got := wide.coldToMs(q, overlapMs)
		assert.Equal(t, int64(800_000), got)
		assert.LessOrEqual(t, got, q.FromMs, "the caller skips the LIST when coldTo <= from")
	})
}
