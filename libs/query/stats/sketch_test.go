package stats

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exactRank returns the value of rank floor(q·(n−1)) of sorted values.
func exactRank(sorted []int64, q float64) int64 {
	return sorted[int(math.Floor(q*float64(len(sorted)-1)))]
}

// TestSketchRelativeError pins the 02 §2.9 bound: every reported percentile
// is within 1% of the exact value of its rank, over several distributions.
func TestSketchRelativeError(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	dists := map[string]func() int64{
		"uniform 1..10s":   func() int64 { return 1 + rng.Int64N(10_000) },
		"log-normal":       func() int64 { return int64(math.Exp(rng.NormFloat64()*2 + 5)) },
		"heavy tail":       func() int64 { return int64(1 / (rng.Float64() + 1e-6)) },
		"small values":     func() int64 { return rng.Int64N(5) },
		"one huge outlier": func() int64 { return 100 },
	}
	for name, gen := range dists {
		t.Run(name, func(t *testing.T) {
			var s Sketch
			values := make([]int64, 0, 20_000)
			for i := 0; i < 20_000; i++ {
				v := gen()
				values = append(values, v)
				s.Add(v)
			}
			if name == "one huge outlier" {
				values = append(values, math.MaxInt32)
				s.Add(math.MaxInt32)
			}
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			for _, q := range []float64{0, 0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 0.999, 1} {
				got, ok := s.Quantile(q)
				require.True(t, ok)
				want := float64(exactRank(values, q))
				if want == 0 {
					assert.Zero(t, got, "q=%v", q)
					continue
				}
				assert.LessOrEqual(t, math.Abs(got-want)/want, relativeAccuracy+1e-12, "q=%v got %v want %v", q, got, want)
			}
		})
	}
}

func TestSketchRankRule(t *testing.T) {
	var s Sketch
	for _, v := range []int64{10, 20, 30, 40} {
		s.Add(v)
	}
	// floor(0.5·3) = 1, so p50 is the second value, not an interpolation.
	assert.Equal(t, int64(20), *s.QuantileMs(0.5))
	assert.Equal(t, int64(40), *s.QuantileMs(1))
	assert.Equal(t, int64(10), *s.QuantileMs(0))

	var empty Sketch
	assert.Nil(t, empty.QuantileMs(0.5), "an empty group has null percentiles")
}

func TestSketchZeroAndNegative(t *testing.T) {
	var s Sketch
	assert.True(t, s.Add(0), "the zero bin opens")
	assert.False(t, s.Add(-5), "a negative duration counts as zero")
	s.Add(7)
	assert.Equal(t, int64(3), s.Count())
	assert.Equal(t, 2, s.Bins())
	assert.Equal(t, int64(0), *s.QuantileMs(0.5))
	assert.Equal(t, int64(7), *s.QuantileMs(1))
}

// TestSketchMergeIsLossless: two sketches merged report what one sketch over
// every value reports.
func TestSketchMergeIsLossless(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	var a, b, whole Sketch
	for i := 0; i < 5000; i++ {
		v := rng.Int64N(100_000)
		whole.Add(v)
		if i%3 == 0 {
			a.Add(v)
		} else {
			b.Add(v)
		}
	}
	a.Merge(&b)
	assert.Equal(t, whole.JSON(), a.JSON())
	for _, q := range []float64{0.5, 0.95, 0.99} {
		wq, _ := whole.Quantile(q)
		aq, _ := a.Quantile(q)
		assert.Equal(t, wq, aq)
	}
}

func TestSketchJSONRoundTrip(t *testing.T) {
	var s Sketch
	for _, v := range []int64{0, 0, 1, 5, 5, 1000, 123456} {
		s.Add(v)
	}
	back, ok := s.JSON().Sketch()
	require.True(t, ok)
	assert.Equal(t, s.JSON(), back.JSON())
	assert.Equal(t, s.Count(), back.Count())

	_, ok = SketchJSON{Index: []int32{1}, Counts: nil}.Sketch()
	assert.False(t, ok, "parallel arrays of different lengths are malformed")
	_, ok = SketchJSON{Index: []int32{1}, Counts: []int64{-1}}.Sketch()
	assert.False(t, ok, "a negative count is malformed")
}
