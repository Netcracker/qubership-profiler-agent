// Package stats holds the tier-independent part of GET /api/v1/stats
// (02-read-contract.md §2.9) and of its collector side /internal/v1/stats
// (§3): request parsing, the DDSketch the percentiles come from, the bounded
// per-source aggregator, the PK hashes that keep a call from counting in both
// tiers, the merge across sources, and the wire shapes. It does no I/O, so
// the collector and the query service aggregate with the same code.
package stats

import (
	"math"
	"sort"
)

// relativeAccuracy is the DDSketch bound of 02 §2.9: a reported percentile is
// within 1% of the exact value of its rank, before rounding to whole
// milliseconds.
const relativeAccuracy = 0.01

var (
	gamma    = (1 + relativeAccuracy) / (1 - relativeAccuracy)
	logGamma = math.Log(gamma)
)

// Sketch is a DDSketch over durations in whole milliseconds. Bin i holds the
// values in (γ^(i-1), γ^i], and one more bin holds zero, so every bin
// reports a value within relativeAccuracy of each value it holds. Sketches
// merge without loss by adding bin counts. The zero value is empty and ready
// to use.
type Sketch struct {
	zero  int64
	bins  map[int32]int64
	count int64
}

// binIndex returns the logarithmic bin of a positive value.
func binIndex(v float64) int32 {
	return int32(math.Ceil(math.Log(v) / logGamma))
}

// binValue is the value a bin reports: the point whose relative distance to
// both bin bounds is relativeAccuracy.
func binValue(i int32) float64 {
	return 2 * math.Pow(gamma, float64(i)) / (gamma + 1)
}

// Add records one duration. A negative duration counts as zero. It reports
// whether the call opened a bin, which is what a memory charge tracks.
func (s *Sketch) Add(durationMs int64) (newBin bool) {
	return s.addCount(durationMs, 1)
}

func (s *Sketch) addCount(durationMs, n int64) bool {
	s.count += n
	if durationMs <= 0 {
		newBin := s.zero == 0
		s.zero += n
		return newBin
	}
	if s.bins == nil {
		s.bins = map[int32]int64{}
	}
	i := binIndex(float64(durationMs))
	_, had := s.bins[i]
	s.bins[i] += n
	return !had
}

// Merge adds o's bins to s and returns how many bins it opened.
func (s *Sketch) Merge(o *Sketch) (newBins int) {
	if o.zero > 0 {
		if s.zero == 0 {
			newBins++
		}
		s.zero += o.zero
	}
	if len(o.bins) > 0 && s.bins == nil {
		s.bins = make(map[int32]int64, len(o.bins))
	}
	for i, n := range o.bins {
		if _, had := s.bins[i]; !had {
			newBins++
		}
		s.bins[i] += n
	}
	s.count += o.count
	return newBins
}

// Count returns the number of recorded values.
func (s *Sketch) Count() int64 { return s.count }

// Bins returns the number of non-empty bins, the zero bin included.
func (s *Sketch) Bins() int {
	n := len(s.bins)
	if s.zero > 0 {
		n++
	}
	return n
}

// Quantile returns the value of rank floor(q·(n−1)) in ascending order, the
// rank 02 §2.9 pins, and false for an empty sketch.
func (s *Sketch) Quantile(q float64) (float64, bool) {
	if s.count == 0 {
		return 0, false
	}
	q = math.Max(0, math.Min(1, q))
	rank := int64(math.Floor(q * float64(s.count-1)))
	if rank < s.zero {
		return 0, true
	}
	seen := s.zero
	keys := s.sortedKeys()
	for _, i := range keys {
		seen += s.bins[i]
		if rank < seen {
			return binValue(i), true
		}
	}
	return binValue(keys[len(keys)-1]), true
}

// QuantileMs is Quantile rounded to whole milliseconds, nil for an empty
// sketch: the p50_ms, p95_ms, and p99_ms members of a /stats row.
func (s *Sketch) QuantileMs(q float64) *int64 {
	v, ok := s.Quantile(q)
	if !ok {
		return nil
	}
	ms := int64(math.Round(v))
	return &ms
}

func (s *Sketch) sortedKeys() []int32 {
	keys := make([]int32, 0, len(s.bins))
	for i := range s.bins {
		keys = append(keys, i)
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
	return keys
}

// SketchJSON is the wire form of a sketch in /internal/v1/stats: the zero
// bin's count, then the logarithmic bins as parallel arrays in ascending
// index order.
type SketchJSON struct {
	Zero   int64   `json:"zero"`
	Index  []int32 `json:"index"`
	Counts []int64 `json:"counts"`
}

// JSON renders the sketch for the wire.
func (s *Sketch) JSON() SketchJSON {
	keys := s.sortedKeys()
	out := SketchJSON{Zero: s.zero, Index: keys, Counts: make([]int64, len(keys))}
	for k, i := range keys {
		out.Counts[k] = s.bins[i]
	}
	return out
}

// Sketch rebuilds a sketch from its wire form. Malformed input (arrays of
// different lengths, a negative count) yields false.
func (j SketchJSON) Sketch() (Sketch, bool) {
	if len(j.Index) != len(j.Counts) || j.Zero < 0 {
		return Sketch{}, false
	}
	s := Sketch{zero: j.Zero, count: j.Zero}
	if len(j.Index) > 0 {
		s.bins = make(map[int32]int64, len(j.Index))
	}
	for k, i := range j.Index {
		n := j.Counts[k]
		if n < 0 {
			return Sketch{}, false
		}
		if n == 0 {
			continue
		}
		s.bins[i] += n
		s.count += n
	}
	return s, true
}
