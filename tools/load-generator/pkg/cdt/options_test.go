package cdt

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkloadMapping: every §4 knob of FleetOptions lands on the matching
// vdumper.Workload field, including honored zero shares (the T3 idle shape
// must not fall back to the defaults through a partial zero value).
func TestWorkloadMapping(t *testing.T) {
	opts := FleetOptions{
		DurationThresholds: "50ms,2s",
		DurationShares:     "0.5,0.4,0.1",
		StackDepth:         7,
		SqlShare:           0.3,
		SqlBytes:           2048,
		SqlDedup:           0.8,
		XmlShare:           0.1,
		XmlBytes:           512,
		SuspendRate:        1.5,
		ErrorShare:         0.02,
		RequestIdShare:     0.9,
		CpuFraction:        0.4,
		WaitFraction:       0.2,
		MemoryBytes:        1024,
		DictGrowthPerMin:   30,
	}
	w, err := opts.workload()
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{50 * time.Millisecond, 2 * time.Second}, w.Duration.Thresholds)
	assert.Equal(t, []float64{0.5, 0.4, 0.1}, w.Duration.Shares)
	assert.Equal(t, 7, w.StackDepthMean)
	assert.Equal(t, vdumper.BigParamSpec{Share: 0.3, MeanBytes: 2048, DedupHitRate: 0.8}, w.Sql)
	assert.Equal(t, vdumper.BigParamSpec{Share: 0.1, MeanBytes: 512}, w.Xml)
	assert.Equal(t, 1.5, w.SuspendPerSec)
	assert.Equal(t, 0.02, w.ErrorShare)
	assert.Equal(t, 0.9, w.RequestIdShare)
	assert.Equal(t, 0.4, w.CpuFraction)
	assert.Equal(t, 0.2, w.WaitFraction)
	assert.Equal(t, 1024, w.MemoryMeanBytes)
	assert.Equal(t, 30.0, w.DictionaryGrowthPerMin)
}

// TestWorkloadDefaultDurationSpec: empty threshold/share strings take the
// default retention tiers instead of failing the parse.
func TestWorkloadDefaultDurationSpec(t *testing.T) {
	w, err := FleetOptions{}.workload()
	require.NoError(t, err)
	assert.Equal(t, vdumper.DefaultWorkload().Duration, w.Duration)
}

func TestWorkloadRejectsBadShares(t *testing.T) {
	_, err := FleetOptions{DurationThresholds: "100ms", DurationShares: "0.5,0.4"}.workload()
	require.Error(t, err, "shares must sum to 1")
}

// TestPodIdentityUniqueAcrossVUs: pod names and workload seeds must differ
// across (VU, index) pairs, or the collector would merge pods and the
// synthetic streams would correlate.
func TestPodIdentityUniqueAcrossVUs(t *testing.T) {
	opts := FleetOptions{Addr: "localhost:1"}.withDefaults()
	w, err := opts.workload()
	require.NoError(t, err)

	names := map[string]bool{}
	seeds := map[int64]bool{}
	for vu := uint64(1); vu <= 3; vu++ {
		for idx := 0; idx < 3; idx++ {
			cfg := opts.podConfig(w, vu, idx, vdumper.NoopStats{})
			assert.False(t, names[cfg.PodName], "duplicate pod name %s", cfg.PodName)
			assert.False(t, seeds[cfg.Seed], "duplicate seed %d", cfg.Seed)
			names[cfg.PodName] = true
			seeds[cfg.Seed] = true
		}
	}
	cfg := opts.podConfig(w, 2, 1, vdumper.NoopStats{})
	assert.Equal(t, "load-svc-2-1", cfg.PodName)
	assert.Equal(t, "load", cfg.Namespace)
}

func TestValidateRequiresAddr(t *testing.T) {
	_, err := FleetOptions{}.withDefaults().validate()
	require.Error(t, err)

	spread, err := FleetOptions{Addr: "localhost:1715"}.withDefaults().validate()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, spread, "default startSpread")
}
