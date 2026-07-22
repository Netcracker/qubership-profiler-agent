package cdt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/emutest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/js/modulestest"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
)

const eventually = 5 * time.Second
const tick = 5 * time.Millisecond

// sampleSink drains the VU sample channel and keeps per-metric totals, the
// way the k6 engine would.
type sampleSink struct {
	mu     sync.Mutex
	ch     chan metrics.SampleContainer
	totals map[string]float64
	counts map[string]int
	tags   map[string]map[string]string // metric -> last seen tags
}

func newSampleSink() *sampleSink {
	s := &sampleSink{
		ch:     make(chan metrics.SampleContainer, 1024),
		totals: map[string]float64{},
		counts: map[string]int{},
		tags:   map[string]map[string]string{},
	}
	go func() {
		for sc := range s.ch {
			s.mu.Lock()
			for _, sample := range sc.GetSamples() {
				s.totals[sample.Metric.Name] += sample.Value
				s.counts[sample.Metric.Name]++
				s.tags[sample.Metric.Name] = sample.Tags.Map()
			}
			s.mu.Unlock()
		}
	}()
	return s
}

func (s *sampleSink) total(metric string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totals[metric]
}

func (s *sampleSink) count(metric string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[metric]
}

func (s *sampleSink) lastTags(metric string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tags[metric]
}

// newFleetInstance builds the module the way k6 does: instantiate in the init
// context (metrics registration), then move the VU to the run context.
func newFleetInstance(t *testing.T, ctx context.Context, sink *sampleSink) *ModuleInstance {
	t.Helper()
	rt := modulestest.NewRuntime(t)
	mi, ok := New().NewModuleInstance(rt.VU).(*ModuleInstance)
	require.True(t, ok)

	registry := rt.VU.InitEnvField.Registry
	rt.MoveToVUContext(&lib.State{
		VUID:    1,
		Samples: sink.ch,
		Tags:    lib.NewVUStateTags(registry.RootTagSet().With("scenario", "test")),
		Logger:  logrus.New(),
	})
	rt.VU.CtxField = ctx
	return mi
}

// TestRunFleetDrivesPods: runFleet holds one connection per pod on the
// scripted collector, emits the connect and session-ready samples, and
// reports the fleet totals after a graceful stop.
func TestRunFleetDrivesPods(t *testing.T) {
	col := emutest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newSampleSink()
	mi := newFleetInstance(t, ctx, sink)

	type result struct {
		summary FleetSummary
		err     error
	}
	done := make(chan result, 1)
	go func() {
		summary, err := mi.RunFleet(FleetOptions{
			Addr:        col.Addr(),
			Pods:        2,
			DictInitial: 8,
			StartSpread: "1ms",
		})
		done <- result{summary, err}
	}()

	require.Eventually(t, func() bool { return col.Connections() == 2 },
		eventually, tick, "each pod opens its own connection")
	require.Eventually(t, func() bool { return sink.total("vdumper_connects") == 2 },
		eventually, tick, "both pods report a completed session setup")
	assert.Equal(t, 2, sink.count("vdumper_session_ready_time"), "session-ready fires once per connect")
	assert.Equal(t, 2, sink.count("vdumper_tcp_connect_time"), "the dial time is sampled per connect")

	cancel()
	select {
	case r := <-done:
		require.NoError(t, r.err)
		assert.Equal(t, 2, r.summary.Pods)
		assert.EqualValues(t, 2, r.summary.Connects)
		assert.Zero(t, r.summary.PodFailures)
		assert.Zero(t, r.summary.AckErrors)
	case <-time.After(eventually):
		t.Fatal("runFleet must return after the VU context is cancelled")
	}

	assert.Positive(t, sink.total("vdumper_sent_bytes"), "stream setup sends params bytes")
	if tags := sink.lastTags("vdumper_sent_bytes"); assert.NotNil(t, tags) {
		assert.NotEmpty(t, tags["stream"], "sent bytes carry the stream tag")
		assert.Equal(t, "test", tags["scenario"], "VU tags propagate to fleet samples")
	}
}

// TestRunFleetRequiresVUContext: calling runFleet in the init context is a
// scripting error, reported as such instead of a panic.
func TestRunFleetRequiresVUContext(t *testing.T) {
	rt := modulestest.NewRuntime(t)
	mi, ok := New().NewModuleInstance(rt.VU).(*ModuleInstance)
	require.True(t, ok)
	_, err := mi.RunFleet(FleetOptions{Addr: "localhost:1"})
	require.ErrorContains(t, err, "default function")
}
