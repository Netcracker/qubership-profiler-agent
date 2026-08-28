package vdumper_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/emutest"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the DumperThread lifecycle (virtual-dumper.md §1.1): stream
// setup, the 5 s flush cadence, the ACK_ERROR → drop window → reconnect path
// with the resetRequired=1 dictionary resend, rotation, and graceful close.
// The collector side is the scripted emutest double; time is a fake clock.

const eventually = 5 * time.Second
const tick = 5 * time.Millisecond

// streamOrder is the Dumper.initStreams open order the contract fixes (§2.2).
var streamOrder = []string{
	model.StreamTrace, model.StreamCalls, model.StreamXml, model.StreamSql,
	model.StreamDictionary, model.StreamSuspend, model.StreamParams,
}

type statsRec struct {
	mu           sync.Mutex
	connected    int
	disconnected int
	churned      int
	ackErrors    int
	dropped      int
	tcpConnects  int
	sessionReady int
	ackFlushes   int
	lastErr      error
}

func (r *statsRec) Connected(int) { r.mu.Lock(); defer r.mu.Unlock(); r.connected++ }
func (r *statsRec) Disconnected(_ int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disconnected++
	r.lastErr = err
}
func (r *statsRec) Churned(int) { r.mu.Lock(); defer r.mu.Unlock(); r.churned++ }

func (r *statsRec) churnedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.churned
}
func (r *statsRec) StreamOpened(string, int, bool) {}
func (r *statsRec) BytesSent(string, int)          {}
func (r *statsRec) AckError()                      { r.mu.Lock(); defer r.mu.Unlock(); r.ackErrors++ }
func (r *statsRec) Dropped(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropped += n
}
func (r *statsRec) TcpConnected(time.Duration) { r.mu.Lock(); defer r.mu.Unlock(); r.tcpConnects++ }
func (r *statsRec) SessionReady(time.Duration) { r.mu.Lock(); defer r.mu.Unlock(); r.sessionReady++ }
func (r *statsRec) AckFlushed(string, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ackFlushes++
}

func (r *statsRec) latencyCounts() (tcpConnects, sessionReady, ackFlushes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tcpConnects, r.sessionReady, r.ackFlushes
}

func (r *statsRec) droppedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

func (r *statsRec) snapshot() (connected, disconnected, ackErrors int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connected, r.disconnected, r.ackErrors
}

// quietWorkload pins every dumper-side shape source to zero (no dictionary
// growth, no suspend pauses), so lifecycle tests observe only the protocol.
func quietWorkload() vdumper.Workload {
	return vdumper.Workload{
		Duration: vdumper.DurationSpec{
			Shares: []float64{1},
			Floor:  30 * time.Millisecond,
			Cap:    31 * time.Millisecond,
		},
		StackDepthMean: 1,
	}
}

func startDumper(t *testing.T, col *emutest.Collector, clk *fakeClock, rec *statsRec) (context.CancelFunc, <-chan error) {
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
		DictionaryInitial: 12,
		Workload:          quietWorkload(),
		Clock:             clk,
		Stats:             rec,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- vdumper.New(cfg).Run(ctx) }()
	return cancel, done
}

func initsOf(col *emutest.Collector, conn int) []emutest.Event {
	var out []emutest.Event
	for _, e := range col.EventsOf(model.COMMAND_INIT_STREAM_V2) {
		if e.Conn == conn {
			out = append(out, e)
		}
	}
	return out
}

// decodePhrases splits phrase frames ([fixed-int length][body]) and decodes
// the bodies as a run of var-strings (the dictionary encoding).
func decodePhrases(t *testing.T, payload []byte) []string {
	t.Helper()
	var words []string
	for len(payload) > 0 {
		require.GreaterOrEqual(t, len(payload), 4, "phrase length prefix")
		n := int(binary.BigEndian.Uint32(payload))
		payload = payload[4:]
		require.GreaterOrEqual(t, len(payload), n, "phrase body")
		body := payload[:n]
		payload = payload[n:]
		for len(body) > 0 {
			var units uint64
			shift := 0
			for {
				b := body[0]
				body = body[1:]
				units |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
				shift += 7
			}
			raw := body[:2*units]
			body = body[2*units:]
			u16 := make([]uint16, units)
			for i := range u16 {
				u16[i] = binary.BigEndian.Uint16(raw[2*i:])
			}
			words = append(words, string(utf16.Decode(u16)))
		}
	}
	return words
}

func waitTimer(t *testing.T, clk *fakeClock) {
	t.Helper()
	require.Eventually(t, func() bool { return clk.Waiters() >= 1 },
		eventually, tick, "the dumper must park on a timer")
}

// TestLifecycleOpensSevenStreams: a fresh connection opens exactly the seven
// contract streams in Dumper.initStreams order, sends the params payload
// one-shot at open, and pushes headers plus the dictionary at the first 5 s
// flush cycle.
func TestLifecycleOpensSevenStreams(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
	rec := &statsRec{}
	_, _ = startDumper(t, col, clk, rec)

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 },
		eventually, tick, "seven streams must open")
	inits := initsOf(col, 0)
	for i, e := range inits {
		assert.Equal(t, streamOrder[i], e.Stream, "stream open order must match Dumper.initStreams")
		assert.Equal(t, e.Stream == model.StreamDictionary, e.Reset,
			"only the dictionary opens with resetRequired on a fresh connection")
		assert.Equal(t, 0, e.SeqId, "every stream starts its rolling sequence at 0")
	}

	// The params one-shot flushed during setup: payload present, one
	// REQUEST_ACK_FLUSH so far.
	require.Eventually(t, func() bool { return len(col.StreamData(0, model.StreamParams)) > 0 },
		eventually, tick)
	params := col.StreamData(0, model.StreamParams)
	assert.Equal(t, byte(1), params[4], "params phrase opens with the format-version byte")
	assert.Len(t, col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH), 1)

	// First flush cycle: headers and the dictionary words go out.
	waitTimer(t, clk)
	clk.Advance(5 * time.Second)
	require.Eventually(t, func() bool { return len(col.StreamData(0, model.StreamDictionary)) > 0 },
		eventually, tick, "the dictionary must go out with the first flush cycle")

	words := decodePhrases(t, col.StreamData(0, model.StreamDictionary))
	assert.Len(t, words, 12, "the full initial dictionary must be sent")

	trace := col.StreamData(0, model.StreamTrace)
	require.Len(t, trace, 8, "the trace file header is the 8-byte start epoch")
	assert.Equal(t, uint64(clk.Now().Add(-5*time.Second).UnixMilli()), binary.BigEndian.Uint64(trace),
		"the trace header carries the process-start epoch")

	calls := col.StreamData(0, model.StreamCalls)
	require.Len(t, calls, 16, "the calls header is magic+version plus the base epoch")
	assert.Equal(t, uint64(0xFFFEFDFC)<<32|4, binary.BigEndian.Uint64(calls[:8]))

	assert.Len(t, col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH), 7,
		"a flush cycle adds one REQUEST_ACK_FLUSH per open stream (params is done)")
	connected, disconnected, _ := rec.snapshot()
	assert.Equal(t, 1, connected)
	assert.Equal(t, 0, disconnected)
	tcpConnects, sessionReady, ackFlushes := rec.latencyCounts()
	assert.Equal(t, 1, tcpConnects, "one dial, one TcpConnected sample")
	assert.Equal(t, 1, sessionReady, "SessionReady fires once per incarnation")
	assert.Equal(t, 6, ackFlushes,
		"the flush cycle drains acks once per open stream (params is done)")
}

// TestAckErrorReconnectsAndResendsDictionary: ACK_ERROR_MAGIC tears the
// incarnation down, the dumper waits RestartInterval (10 s), reconnects, and
// re-sends the whole dictionary with resetRequired=1 (G3–G5).
func TestAckErrorReconnectsAndResendsDictionary(t *testing.T) {
	col := emutest.Start(t)
	col.AckOf = func(conn, ackSeq int) emutest.Ack {
		// Refuse the params one-shot's REQUEST_ACK_FLUSH ack on the first
		// connection; every later ack is fine.
		return emutest.Ack{Refuse: conn == 0 && ackSeq == 1}
	}
	clk := newFakeClock()
	rec := &statsRec{}
	_, _ = startDumper(t, col, clk, rec)

	// The incarnation dies during stream setup and parks on the restart timer.
	require.Eventually(t, func() bool {
		_, disconnected, ackErrors := rec.snapshot()
		return disconnected == 1 && ackErrors == 1
	}, eventually, tick, "ACK_ERROR must be recorded as a typed refusal")
	waitTimer(t, clk)
	assert.Equal(t, 1, col.Connections(), "no reconnect before RestartInterval elapses")

	clk.Advance(10 * time.Second)
	require.Eventually(t, func() bool { return len(initsOf(col, 1)) == 7 },
		eventually, tick, "the second incarnation must re-open all seven streams")
	for _, e := range initsOf(col, 1) {
		assert.Equal(t, 0, e.SeqId, "rolling sequences restart with the new pod-restart")
		if e.Stream == model.StreamDictionary {
			assert.True(t, e.Reset, "the dictionary re-opens with resetRequired=1 after a reconnect")
		}
	}

	// The full dictionary goes out again on the new connection.
	waitTimer(t, clk)
	clk.Advance(5 * time.Second)
	require.Eventually(t, func() bool { return len(col.StreamData(1, model.StreamDictionary)) > 0 },
		eventually, tick)
	words := decodePhrases(t, col.StreamData(1, model.StreamDictionary))
	assert.Len(t, words, 12, "the dictionary must be re-sent from word 0")
}

// TestFlushCadence: every FlushInterval adds one REQUEST_ACK_FLUSH per open
// stream — six once the params one-shot is done (G6).
func TestFlushCadence(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
	_, _ = startDumper(t, col, clk, &statsRec{})

	require.Eventually(t, func() bool {
		return len(col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH)) == 1
	}, eventually, tick, "stream setup ends with the params one-shot flush")

	for cycle := 1; cycle <= 2; cycle++ {
		waitTimer(t, clk)
		clk.Advance(5 * time.Second)
		want := 1 + 6*cycle
		require.Eventually(t, func() bool {
			return len(col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH)) == want
		}, eventually, tick, "cycle %d must add six per-stream flushes", cycle)
	}
}

// TestRotationContinuesSequence: when the collector's rotation period
// elapses, the stream re-opens with the next rolling sequence id and no
// dictionary-style reset.
func TestRotationContinuesSequence(t *testing.T) {
	col := emutest.Start(t)
	col.InitReplyOf = func(conn int, stream string) emutest.InitReply {
		if stream == model.StreamTrace {
			return emutest.InitReply{RotationPeriodMs: 3000}
		}
		return emutest.InitReply{}
	}
	clk := newFakeClock()
	_, _ = startDumper(t, col, clk, &statsRec{})

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)

	// At the 5 s wake-up the 3 s rotation period has passed.
	waitTimer(t, clk)
	clk.Advance(5 * time.Second)
	require.Eventually(t, func() bool {
		var traceInits []emutest.Event
		for _, e := range initsOf(col, 0) {
			if e.Stream == model.StreamTrace {
				traceInits = append(traceInits, e)
			}
		}
		return len(traceInits) == 2 && traceInits[1].SeqId == 1 && !traceInits[1].Reset
	}, eventually, tick, "the trace stream must rotate to sequence 1 without a reset")
}

// TestGracefulClose: cancelling the context flushes what is pending and
// announces COMMAND_CLOSE; Run returns nil.
func TestGracefulClose(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
	cancel, done := startDumper(t, col, clk, &statsRec{})

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(eventually):
		t.Fatal("Run must return after ctx cancellation")
	}
	require.Eventually(t, func() bool { return len(col.EventsOf(model.COMMAND_CLOSE)) > 0 },
		eventually, tick, "the agent announces a graceful close")
	words := decodePhrases(t, col.StreamData(0, model.StreamDictionary))
	assert.Len(t, words, 12, "the shutdown flush pushes the pending dictionary out")
}

// TestChurnCyclesAbruptly (churn mode, virtual-dumper.md §1.1): a healthy
// incarnation disconnects on purpose after ChurnInterval — abruptly, with no
// COMMAND_CLOSE — reconnects after RestartInterval under the same pod name,
// re-sends the dictionary with resetRequired=1, and the cycle counts through
// Churned, never through Disconnected or the ack-error counter.
func TestChurnCyclesAbruptly(t *testing.T) {
	col := emutest.Start(t)
	clk := newFakeClock()
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
		DictionaryInitial: 12,
		Workload:          quietWorkload(),
		ChurnInterval:     30 * time.Second,
		Clock:             clk,
		Stats:             rec,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = vdumper.New(cfg).Run(ctx) }()

	require.Eventually(t, func() bool { return len(initsOf(col, 0)) == 7 }, eventually, tick)

	// Past the jittered churn deadline (30 s ± 20%) the incarnation ends.
	waitTimer(t, clk)
	clk.Advance(40 * time.Second)
	require.Eventually(t, func() bool { return rec.churnedCount() == 1 },
		eventually, tick, "the churn cycle must be counted")
	assert.Empty(t, col.EventsOf(model.COMMAND_CLOSE),
		"churn disconnects abruptly: no COMMAND_CLOSE, unlike a graceful shutdown")
	connected, disconnected, ackErrors := rec.snapshot()
	assert.Equal(t, 1, connected)
	assert.Equal(t, 0, disconnected, "a deliberate cycle is not a failure reconnect")
	assert.Equal(t, 0, ackErrors)

	// The ordinary RestartInterval reconnect path follows, with the full
	// dictionary resend under resetRequired=1.
	waitTimer(t, clk)
	clk.Advance(10 * time.Second)
	require.Eventually(t, func() bool { return len(initsOf(col, 1)) == 7 },
		eventually, tick, "the pod must reconnect after RestartInterval")
	for _, e := range initsOf(col, 1) {
		if e.Stream == model.StreamDictionary {
			assert.True(t, e.Reset, "every churn cycle re-sends the dictionary with resetRequired=1")
		}
	}
	waitTimer(t, clk)
	clk.Advance(5 * time.Second)
	require.Eventually(t, func() bool { return len(col.StreamData(1, model.StreamDictionary)) > 0 },
		eventually, tick)
	words := decodePhrases(t, col.StreamData(1, model.StreamDictionary))
	assert.Len(t, words, 12, "the dictionary is re-sent from word 0 on every cycle")
}

// TestBlacklistedStops: BLACK_LISTED_RESP stops the pod permanently — the
// agent never reconnects a blacklisted namespace.
func TestBlacklistedStops(t *testing.T) {
	col := emutest.Start(t)
	col.HandshakeVersion = func(uint64) uint64 { return model.BLACK_LISTED_RESP }
	clk := newFakeClock()
	_, done := startDumper(t, col, clk, &statsRec{})

	select {
	case err := <-done:
		require.ErrorIs(t, err, vdumper.ErrBlacklisted)
	case <-time.After(eventually):
		t.Fatal("Run must stop on a blacklisted namespace")
	}
	assert.Equal(t, 1, col.Connections())
}
