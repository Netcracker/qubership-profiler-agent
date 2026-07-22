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

// The trace-pipeline tests pin G1 and G2 (virtual-dumper.md §2.5): producer
// goroutines model app threads, their buffers serialize as logical chunks
// that interleave on the wire, and every closed root call emits a calls
// record whose (file index, buffer offset, record index) points back into the
// trace bytes. Decoding runs through libs/parser/pipe — the collector's own
// readers — so what the double collected is what the collector could parse.

func startLoadedDumper(t *testing.T, col *emutest.Collector, clk *fakeClock) {
	t.Helper()
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
		DictionaryInitial:    16,
		ThreadsPerPod:        2,
		CallsPerSecPerThread: 2,
		ChunkMaxBytes:        60, // hand off after a few calls to force interleaving
		Workload:             quietWorkload(),
		Clock:                clk,
		Stats:                &statsRec{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = vdumper.New(cfg).Run(ctx) }()
}

// advanceLoaded steps fake time by one second at a time, waiting for the pump
// and both producers to park between steps so no timer registration races the
// advance.
func advanceLoaded(t *testing.T, clk *fakeClock, steps int) {
	t.Helper()
	for i := 0; i < steps; i++ {
		require.Eventually(t, func() bool { return clk.Waiters() >= 3 },
			eventually, tick, "the pump and both producers must park on timers")
		clk.Advance(time.Second)
	}
}

func TestTracePipelineInterleavesThreads(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
	startLoadedDumper(t, col, clk)

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)
	advanceLoaded(t, clk, 16)

	var items []pipe.TraceItem
	require.Eventually(t, func() bool {
		data := col.StreamData(0, model.StreamTrace)
		if len(data) <= 8 {
			return false
		}
		items = nil
		for it := range pipe.TracesPipeReader(context.Background(), pipe.NewPipeReader(bytes.NewReader(data), true)) {
			items = append(items, it)
		}
		return len(items) >= 4
	}, eventually, tick, "several chunks from both threads must reach the collector")

	byThread := map[uint64]int{}
	transitions := map[[2]uint64]bool{}
	for i, it := range items {
		assert.True(t, it.Complete, "chunk #%d must close with EVENT_FINISH_RECORD", i)
		byThread[it.ThreadId]++
		if i > 0 && items[i-1].ThreadId != it.ThreadId {
			transitions[[2]uint64{items[i-1].ThreadId, it.ThreadId}] = true
		}
	}
	require.Len(t, byThread, 2, "both producer threads must appear on the wire")
	assert.GreaterOrEqual(t, len(transitions), 2,
		"chunks must interleave in both directions, not arrive thread-by-thread: %v", transitions)
}

func TestCallsRecordsLinkIntoTraceChunks(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
	startLoadedDumper(t, col, clk)

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)
	advanceLoaded(t, clk, 16)

	var traces []pipe.TraceItem
	var calls []pipe.CallItem
	require.Eventually(t, func() bool {
		traceData := col.StreamData(0, model.StreamTrace)
		callsData := col.StreamData(0, model.StreamCalls)
		if len(traceData) <= 8 || len(callsData) <= 16 {
			return false
		}
		traces = nil
		for it := range pipe.TracesPipeReader(context.Background(), pipe.NewPipeReader(bytes.NewReader(traceData), true)) {
			traces = append(traces, it)
		}
		calls = nil
		for it := range pipe.CallsPipeReader(context.Background(), pipe.NewPipeReader(bytes.NewReader(callsData), true)) {
			calls = append(calls, it)
		}
		return len(traces) >= 2 && len(calls) >= 4
	}, eventually, tick, "trace chunks and calls records must both arrive")

	// Chunk offset → owning thread, from the trace stream itself.
	chunkThread := map[int]uint64{}
	for _, it := range traces {
		chunkThread[int(it.Offset)] = it.ThreadId
	}
	threadName := map[uint64]string{1000: "exec-0", 1001: "exec-1"}

	for i, c := range calls {
		call := c.Call
		assert.Equal(t, 1, call.TraceFileIndex,
			"record #%d must reference the first trace file of this connection", i)
		owner, ok := chunkThread[call.BufferOffset]
		require.True(t, ok,
			"record #%d buffer_offset %d must match a decoded chunk start (chunks at %v)",
			i, call.BufferOffset, chunkThread)
		assert.Equal(t, threadName[owner], call.ThreadName,
			"record #%d must carry the thread of the chunk it points into", i)
		assert.EqualValues(t, 30, call.Duration, "the quiet workload runs fixed 30 ms calls")
		assert.EqualValues(t, 1, call.Calls, "depth-1 stack: the enter counter includes only the root")
	}
}
