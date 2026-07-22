package vdumper_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/emutest"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shape tests pin G7–G9 (virtual-dumper.md §4): the workload knobs land
// within statistical tolerance on the wire, and every stream the pod produces
// decodes through libs/parser/pipe. The clock is fake and the seeds are
// fixed, so the sampled shape is reproducible run to run.

// Well-known dictionary ids: the workload's param words occupy the first
// dictionary positions (request.id, call.red, sql, xml).
const (
	tagRequestId = 0
	tagCallRed   = 1
)

func startShapedDumper(t *testing.T, col *emutest.Collector, clk *fakeClock, w vdumper.Workload,
	threads int, callsPerSec float64) *statsRec {
	t.Helper()
	rec := &statsRec{}
	cfg := vdumper.Config{
		Namespace: "ns", Service: "svc", PodName: "pod-1",
		Connection: emulator.ConnectionOpts{
			ProtocolAddress: col.Addr(),
			Timeout: profio.TcpTimeout{
				ConnectTimeout: 2 * time.Second,
				SessionTimeout: time.Minute,
				ReadTimeout:    2 * time.Second,
				WriteTimeout:   2 * time.Second,
			},
		},
		DictionaryInitial:    64,
		ThreadsPerPod:        threads,
		CallsPerSecPerThread: callsPerSec,
		Workload:             w,
		Clock:                clk,
		Stats:                rec,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = vdumper.New(cfg).Run(ctx) }()
	return rec
}

// advanceShaped steps fake time, waiting for the pump plus every producer to
// park between steps.
func advanceShaped(t *testing.T, clk *fakeClock, parked, steps int) {
	t.Helper()
	for i := 0; i < steps; i++ {
		require.Eventually(t, func() bool { return clk.Waiters() == parked },
			eventually, tick, "the pump and all producers must park before an advance")
		clk.Advance(time.Second)
	}
}

func decodeCalls(t *testing.T, data []byte) []pipe.CallItem {
	t.Helper()
	var out []pipe.CallItem
	for it := range pipe.CallsPipeReader(context.Background(), pipe.NewPipeReader(bytes.NewReader(data), true)) {
		out = append(out, it)
	}
	return out
}

func TestWorkloadShapeOnTheWire(t *testing.T) {
	w := vdumper.Workload{
		Duration: vdumper.DurationSpec{
			Thresholds: []time.Duration{100 * time.Millisecond, time.Second, 10 * time.Second},
			Shares:     []float64{0.70, 0.20, 0.07, 0.03},
		},
		StackDepthMean:         3,
		RequestIdShare:         1.0,
		Sql:                    vdumper.BigParamSpec{Share: 0.3, MeanBytes: 512, DedupHitRate: 0.9, PoolSize: 50},
		Xml:                    vdumper.BigParamSpec{Share: 0.1, MeanBytes: 1024},
		SuspendPerSec:          2,
		ErrorShare:             0.05,
		DictionaryGrowthPerMin: 60,
	}
	col := emutest.Start(t)
	clk := newFakeClock()
	rec := startShapedDumper(t, col, clk, w, 2, 50)

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)
	advanceShaped(t, clk, 3, 21)

	var calls []pipe.CallItem
	if !assert.Eventually(t, func() bool {
		calls = decodeCalls(t, col.StreamData(0, model.StreamCalls))
		return len(calls) >= 1500
	}, eventually, tick) {
		t.Fatalf("twenty seconds at 2×50 calls/s must produce a large sample; got %d records from %d calls-stream bytes, %d dropped chunks",
			len(calls), len(col.StreamData(0, model.StreamCalls)), rec.droppedCount())
	}

	// G7: duration-class shares within statistical tolerance.
	classes := make([]int, 4)
	withRequestId, withRed := 0, 0
	for _, c := range calls {
		d := time.Duration(c.Call.Duration) * time.Millisecond
		switch {
		case d < 100*time.Millisecond:
			classes[0]++
		case d < time.Second:
			classes[1]++
		case d < 10*time.Second:
			classes[2]++
		default:
			classes[3]++
		}
		if _, ok := c.Call.Params[tagRequestId]; ok {
			withRequestId++
		}
		if _, ok := c.Call.Params[tagCallRed]; ok {
			withRed++
		}
	}
	n := float64(len(calls))
	assert.InDelta(t, 0.70, float64(classes[0])/n, 0.05, "short_clean share")
	assert.InDelta(t, 0.20, float64(classes[1])/n, 0.05, "medium_clean share")
	assert.InDelta(t, 0.07, float64(classes[2])/n, 0.03, "long_clean share")
	assert.InDelta(t, 0.03, float64(classes[3])/n, 0.02, "top-class share")

	// G9: the error and request.id shares.
	assert.Equal(t, len(calls), withRequestId, "every call carries its indexed request.id")
	assert.InDelta(t, 0.05, float64(withRed)/n, 0.02, "call.red share feeds the any_error class")

	// G8: sql deduplicates through the cache — far fewer distinct values reach
	// the wire than calls reference them; xml never deduplicates.
	var sqlValues, xmlValues []pipe.StringItem
	for it := range pipe.StringPipeReader(context.Background(),
		pipe.NewPipeReader(bytes.NewReader(col.StreamData(0, model.StreamSql)), true)) {
		sqlValues = append(sqlValues, it)
	}
	for it := range pipe.StringPipeReader(context.Background(),
		pipe.NewPipeReader(bytes.NewReader(col.StreamData(0, model.StreamXml)), true)) {
		xmlValues = append(xmlValues, it)
	}
	sqlTagged := int(0.3 * n) // expectation, ±binomial noise
	require.NotEmpty(t, sqlValues)
	assert.Less(t, len(sqlValues), sqlTagged/2,
		"a 0.9 dedup hit rate must keep most sql references off the value stream")
	assert.Greater(t, len(xmlValues), int(0.1*n)/2, "xml values are written per reference")

	// G9: dictionary growth appends new words over time.
	words := decodePhrases(t, col.StreamData(0, model.StreamDictionary))
	assert.GreaterOrEqual(t, len(words), 64+10, "60 words/min over ~20 s must append new words")
	assert.Equal(t, "request.id", words[0])
	assert.Equal(t, "call.red", words[1])

	// G8: suspend events flow at the configured rate. Decoded manually: the
	// wire is multi-phrase (one frame per flush window, like the agent), and
	// pipe.SuspendPipeReader still has the documented multi-phrase framing
	// gap (params.go), tracked separately.
	durations := decodeSuspendPairs(t, col.StreamData(0, model.StreamSuspend))
	assert.GreaterOrEqual(t, len(durations), 20, "2 pauses/s over ~20 s")
	for _, d := range durations {
		assert.Positive(t, d)
		assert.LessOrEqual(t, d, 200)
	}

	// G8: the params stream declares the workload's param metadata (a single
	// phrase per connection, which the pipe reader parses fine).
	var params []pipe.ParamItem
	for it := range pipe.ParamsPipeReader(context.Background(),
		pipe.NewPipeReader(bytes.NewReader(col.StreamData(0, model.StreamParams)), true)) {
		params = append(params, it)
	}
	require.Len(t, params, 4)
	assert.Equal(t, "request.id", params[0].Name)
	assert.True(t, params[0].IsIndex)
	assert.Equal(t, "call.red", params[1].Name)

	// The trace stream stays decodable with tags and big-param references in
	// the events (G1 under the full shape).
	complete := 0
	for it := range pipe.TracesPipeReader(context.Background(),
		pipe.NewPipeReader(bytes.NewReader(col.StreamData(0, model.StreamTrace)), true)) {
		if it.Complete {
			complete++
		}
	}
	assert.Greater(t, complete, 10, "shaped chunks must decode to completion")
}

// TestLongCallsKeepRetroactiveStarts: a call of the top duration class ends
// now and started duration ago; its chunk opens at that retroactive start
// instead of clamping the timestamps forward, so no call ends in the future
// and long calls land in their true time buckets.
func TestLongCallsKeepRetroactiveStarts(t *testing.T) {
	w := vdumper.Workload{
		Duration: vdumper.DurationSpec{
			Thresholds: []time.Duration{10 * time.Second},
			Shares:     []float64{0, 1},
			Cap:        40 * time.Second,
		},
		StackDepthMean: 2,
	}
	col := emutest.Start(t)
	clk := newFakeClock()
	startShapedDumper(t, col, clk, w, 1, 0.5)

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)
	advanceShaped(t, clk, 2, 31)

	var calls []pipe.CallItem
	require.Eventually(t, func() bool {
		calls = decodeCalls(t, col.StreamData(0, model.StreamCalls))
		return len(calls) >= 5
	}, eventually, tick)

	nowMs := clk.Now().UnixMilli()
	for i, c := range calls {
		require.GreaterOrEqual(t, int64(c.Call.Duration), int64(10_000),
			"record #%d must be a top-class call", i)
		endMs := c.Time.UnixMilli() + int64(c.Call.Duration)
		assert.LessOrEqual(t, endMs, nowMs,
			"record #%d must not end in the future (retro start, not a clamped one)", i)
	}
	for it := range pipe.TracesPipeReader(context.Background(),
		pipe.NewPipeReader(bytes.NewReader(col.StreamData(0, model.StreamTrace)), true)) {
		assert.True(t, it.Complete, "retro-started chunks must stay parseable")
	}
}

// decodeSuspendPairs strips the phrase framing, skips the 8-byte header, and
// returns the pause durations of the (delta, duration) varint pairs.
func decodeSuspendPairs(t *testing.T, payload []byte) []int {
	t.Helper()
	var body []byte
	for len(payload) > 0 {
		require.GreaterOrEqual(t, len(payload), 4)
		n := int(uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3]))
		payload = payload[4:]
		require.GreaterOrEqual(t, len(payload), n)
		body = append(body, payload[:n]...)
		payload = payload[n:]
	}
	require.GreaterOrEqual(t, len(body), 8, "the suspend stream opens with its 8-byte base time")
	body = body[8:]
	varint := func() int {
		v, shift := 0, 0
		for {
			require.NotEmpty(t, body)
			b := body[0]
			body = body[1:]
			v |= int(b&0x7F) << shift
			if b < 0x80 {
				return v
			}
			shift += 7
		}
	}
	var durations []int
	for len(body) > 0 {
		_ = varint() // delta of the pause end
		durations = append(durations, varint())
	}
	return durations
}
