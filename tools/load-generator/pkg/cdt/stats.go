package cdt

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"go.k6.io/k6/metrics"
)

// statsAdapter maps StatsListener callbacks of a whole fleet to k6 samples.
// Callbacks arrive on dumper goroutines; PushIfNotDone and the atomic
// counters tolerate that, and the tag sets are precomputed so the hot
// BytesSent path allocates nothing.
type statsAdapter struct {
	ctx     context.Context
	samples chan<- metrics.SampleContainer
	m       fleetMetrics
	tags    *metrics.TagSet
	// streamTags carries tags.With("stream", name) for the seven contract
	// streams; unknown names fall back to the untagged set.
	streamTags map[string]*metrics.TagSet

	// Fleet totals for the runFleet summary.
	connects, reconnects, churns, ackErrors, dropped atomic.Int64
}

var _ vdumper.StatsListener = (*statsAdapter)(nil)

func newStatsAdapter(ctx context.Context, samples chan<- metrics.SampleContainer,
	tags *metrics.TagSet, m fleetMetrics) *statsAdapter {

	streams := []string{
		model.StreamTrace, model.StreamCalls, model.StreamXml, model.StreamSql,
		model.StreamDictionary, model.StreamSuspend, model.StreamParams,
	}
	st := make(map[string]*metrics.TagSet, len(streams))
	for _, s := range streams {
		st[s] = tags.With("stream", s)
	}
	return &statsAdapter{ctx: ctx, samples: samples, m: m, tags: tags, streamTags: st}
}

func (a *statsAdapter) push(m *metrics.Metric, tags *metrics.TagSet, value float64) {
	metrics.PushIfNotDone(a.ctx, a.samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: m, Tags: tags},
		Time:       time.Now(),
		Value:      value,
	})
}

func (a *statsAdapter) forStream(stream string) *metrics.TagSet {
	if t, ok := a.streamTags[stream]; ok {
		return t
	}
	return a.tags
}

func (a *statsAdapter) Connected(int) {
	a.connects.Add(1)
	a.push(a.m.connects, a.tags, 1)
}

func (a *statsAdapter) Disconnected(int, error) {
	a.reconnects.Add(1)
	a.push(a.m.reconnects, a.tags, 1)
}

func (a *statsAdapter) Churned(int) {
	a.churns.Add(1)
	a.push(a.m.churns, a.tags, 1)
}

func (a *statsAdapter) StreamOpened(string, int, bool) {}

func (a *statsAdapter) BytesSent(stream string, n int) {
	a.push(a.m.sentBytes, a.forStream(stream), float64(n))
}

func (a *statsAdapter) AckError() {
	a.ackErrors.Add(1)
	a.push(a.m.ackErrors, a.tags, 1)
}

func (a *statsAdapter) Dropped(chunks int) {
	a.dropped.Add(int64(chunks))
	a.push(a.m.droppedChunks, a.tags, float64(chunks))
}

func (a *statsAdapter) TcpConnected(d time.Duration) {
	a.push(a.m.tcpConnectTime, a.tags, metrics.D(d))
}

func (a *statsAdapter) SessionReady(d time.Duration) {
	a.push(a.m.sessionReadyTime, a.tags, metrics.D(d))
}

func (a *statsAdapter) AckFlushed(stream string, d time.Duration) {
	a.push(a.m.ackFlushTime, a.forStream(stream), metrics.D(d))
}
