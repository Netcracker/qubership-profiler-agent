package vdumper

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Workload is the load-shape parameter set of one pod (load-testing-plan.md
// §4). A zero-value Workload takes DefaultWorkload(); a non-zero one is used
// verbatim, so an explicit zero share really means "none of these".
type Workload struct {
	// Duration shapes root-call durations; see DurationSpec.
	Duration DurationSpec
	// StackDepthMean is the mean of the geometric stack-depth distribution
	// (enters per call, root included).
	StackDepthMean int
	// RequestIdShare is the fraction of calls carrying an indexed
	// `request.id` param with a unique value — the indexed-param cardinality
	// driver.
	RequestIdShare float64
	// Sql and Xml shape the big-param streams: sql values deduplicate through
	// the agent's cache, xml values never do.
	Sql BigParamSpec
	Xml BigParamSpec
	// SuspendPerSec is the stop-the-world pause rate fed to the suspend
	// stream.
	SuspendPerSec float64
	// ErrorShare is the fraction of calls tagged `call.red` — the collector's
	// any_error retention class marker.
	ErrorShare float64
	// CpuFraction and WaitFraction derive the per-call cpu/wait counters from
	// the call duration. The defaults are zero: the calibration reference is a
	// sleep-shaped workload whose JMX counters stay flat, and a nonzero
	// counter adds a time.cpu / time.wait tag to every call's trace.
	CpuFraction  float64
	WaitFraction float64
	// MemoryMeanBytes is the mean of the per-call memory.allocated counter
	// (default 4096, the reference workload's magnitude).
	MemoryMeanBytes int
	// DictionaryGrowthPerMin appends this many new dictionary words per
	// minute, modeling new code paths (drives the dictionary stream and
	// collector RAM).
	DictionaryGrowthPerMin float64
}

// DurationSpec samples call durations as explicit shares over duration
// classes: Shares[i] is the fraction of calls landing in
// [Thresholds[i-1], Thresholds[i]), with one more share than thresholds for
// the open top class ending at Cap. Within a class the duration is
// log-uniform. The defaults target the collector's retention tiers; the
// agent's local file classes (100 ms / 500 ms / 3 s / 60 m) make an
// alternative preset.
type DurationSpec struct {
	Thresholds []time.Duration
	Shares     []float64
	// Floor bounds the first class from below (default 1 ms).
	Floor time.Duration
	// Cap bounds the open top class (default 4× the last threshold).
	Cap time.Duration
}

// BigParamSpec shapes one big-param value stream.
type BigParamSpec struct {
	// Share of calls carrying one such value.
	Share float64
	// MeanBytes is the mean value size (log-uniform in [MeanBytes/4,
	// MeanBytes*4)).
	MeanBytes int
	// DedupHitRate is the probability a call reuses an already-seen value
	// instead of a fresh one (sql only; the agent's dedup cache turns reuse
	// into (seq, offset) references).
	DedupHitRate float64
	// PoolSize bounds the distinct-value pool reuse draws from (default 100).
	PoolSize int
}

// DefaultWorkload is the §4 default parameter set.
func DefaultWorkload() Workload {
	return Workload{
		Duration: DurationSpec{
			Thresholds: []time.Duration{100 * time.Millisecond, time.Second, 10 * time.Second},
			Shares:     []float64{0.90, 0.07, 0.025, 0.005},
		},
		StackDepthMean:         10,
		RequestIdShare:         1.0,
		Sql:                    BigParamSpec{Share: 0.2, MeanBytes: 1024, DedupHitRate: 0.9},
		Xml:                    BigParamSpec{Share: 0.05, MeanBytes: 4096},
		SuspendPerSec:          0.5,
		ErrorShare:             0.01,
		DictionaryGrowthPerMin: 10,
		MemoryMeanBytes:        4096,
	}
}

// ParseDurationSpec builds a DurationSpec from the comma-separated CLI/env
// form: thresholds like "100ms,1s,10s" and shares like "0.90,0.07,0.025,0.005".
// Shares must count one more than thresholds (the open top class) and sum
// to 1 within 1%.
func ParseDurationSpec(thresholds, shares string) (DurationSpec, error) {
	var spec DurationSpec
	for _, s := range strings.Split(thresholds, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return DurationSpec{}, fmt.Errorf("bad duration threshold %q: %w", s, err)
		}
		spec.Thresholds = append(spec.Thresholds, d)
	}
	total := 0.0
	for _, s := range strings.Split(shares, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return DurationSpec{}, fmt.Errorf("bad duration share %q: %w", s, err)
		}
		spec.Shares = append(spec.Shares, v)
		total += v
	}
	if len(spec.Shares) != len(spec.Thresholds)+1 {
		return DurationSpec{}, fmt.Errorf("duration shares need %d values for %d thresholds, got %d",
			len(spec.Thresholds)+1, len(spec.Thresholds), len(spec.Shares))
	}
	if total < 0.99 || total > 1.01 {
		return DurationSpec{}, fmt.Errorf("duration shares must sum to 1, got %g", total)
	}
	return spec, nil
}

func (w Workload) isZero() bool {
	return w.Duration.Thresholds == nil && w.Duration.Shares == nil &&
		w.StackDepthMean == 0 && w.RequestIdShare == 0 &&
		w.Sql == (BigParamSpec{}) && w.Xml == (BigParamSpec{}) &&
		w.SuspendPerSec == 0 && w.ErrorShare == 0 && w.DictionaryGrowthPerMin == 0 &&
		w.CpuFraction == 0 && w.WaitFraction == 0 && w.MemoryMeanBytes == 0
}

// sampleMs draws one call duration in milliseconds.
func (s DurationSpec) sampleMs(rnd *rand.Rand) int {
	floor := s.Floor
	if floor == 0 {
		floor = time.Millisecond
	}
	capD := s.Cap
	if capD == 0 && len(s.Thresholds) > 0 {
		capD = 4 * s.Thresholds[len(s.Thresholds)-1]
	}
	class := len(s.Shares) - 1
	r := rnd.Float64()
	for i, share := range s.Shares {
		if r < share {
			class = i
			break
		}
		r -= share
	}
	lo := floor
	if class > 0 {
		lo = s.Thresholds[class-1]
	}
	hi := capD
	if class < len(s.Thresholds) {
		hi = s.Thresholds[class]
	}
	return int(logUniform(rnd, float64(lo.Milliseconds()), float64(hi.Milliseconds())))
}

// jitterDuration spreads d uniformly by ± fraction (the churn deadline
// spread, virtual-dumper.md §1.1).
func jitterDuration(rnd *rand.Rand, d time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return d
	}
	spread := 1 + fraction*(2*rnd.Float64()-1)
	return time.Duration(float64(d) * spread)
}

// logUniform draws from [lo, hi) with a log-uniform density, clamping lo to 1.
func logUniform(rnd *rand.Rand, lo, hi float64) int64 {
	lo = math.Max(lo, 1)
	if hi <= lo {
		return int64(lo)
	}
	v := math.Exp(math.Log(lo) + rnd.Float64()*(math.Log(hi)-math.Log(lo)))
	return int64(math.Min(v, hi-1))
}

// sampleDepth draws a geometric stack depth with the given mean, at least 1.
func sampleDepth(rnd *rand.Rand, mean int) int {
	if mean <= 1 {
		return 1
	}
	depth := 1
	p := 1.0 / float64(mean)
	for depth < 64 && rnd.Float64() > p {
		depth++
	}
	return depth
}

// sampleSize draws a value size around the mean, log-uniform in
// [mean/4, mean*4).
func sampleSize(rnd *rand.Rand, mean int) int {
	if mean <= 4 {
		return max(1, mean)
	}
	return int(logUniform(rnd, float64(mean)/4, float64(mean)*4))
}

// syntheticValue builds a payload of roughly n bytes. The repeated-vocabulary
// shape compresses somewhere between random noise and constant filler; if
// calibration shows a material compressibility gap against production traffic,
// the template fallback of load-testing-plan.md §4 replaces this.
func syntheticValue(rnd *rand.Rand, kind string, n int) string {
	words := [...]string{"select", "from", "where", "join", "order", "group", "value", "column",
		"table", "index", "insert", "update", "delete", "commit", "<item>", "</item>", "attr=\"x\""}
	var b strings.Builder
	b.Grow(n + 16)
	b.WriteString(kind)
	for b.Len() < n {
		b.WriteString(words[rnd.Intn(len(words))])
		b.WriteByte(' ')
		if rnd.Intn(8) == 0 {
			b.WriteString("id")
			b.WriteString(strconv.Itoa(rnd.Intn(100000)))
		}
	}
	return b.String()
}
