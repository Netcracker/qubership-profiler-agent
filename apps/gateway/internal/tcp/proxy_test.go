package tcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
)

func randomBytes(t testing.TB, n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// TestPassthroughIntoRealCollector is the G3 acceptance test: a synthetic
// agent talks through the gateway to a real libs/wire server, and the data
// arrives intact.
func TestPassthroughIntoRealCollector(t *testing.T) {
	l := newRecordingListener()
	p := startProxy(t, baseConfig(startCollector(t, l)), nil)

	ac := agent(t, p.Addr().String(), "ns-1", "billing", "billing-7d9f")
	assert.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion(), "the handshake reply comes from the collector")

	var chunks [][]byte
	var want []byte
	for i := 0; i < 50; i++ {
		c := randomBytes(t, 1024) // the agent's RCV_DATA payload size
		chunks = append(chunks, c)
		want = append(want, c...)
	}
	sendStream(t, ac, model.StreamTrace, chunks...)
	sendStream(t, ac, model.StreamCalls, []byte("calls payload"))

	assert.Equal(t, want, l.received("billing-7d9f/"+model.StreamTrace))
	assert.Equal(t, []byte("calls payload"), l.received("billing-7d9f/"+model.StreamCalls))
	require.Equal(t, 1, l.podCount())
	assert.Equal(t, "ns-1", l.pods[0].Namespace)
	assert.Equal(t, "billing", l.pods[0].Service)
}

func TestUpstreamDownClosesTheAgent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := ln.Addr().String()
	require.NoError(t, ln.Close())

	reader := sdkmetric.NewManualReader()
	p := startProxy(t, baseConfig(dead), nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	c, err := net.Dial("tcp", p.Addr().String())
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	_, err = c.Write(handshakeBytes(t, "ns-1", "svc", "pod-1"))
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = c.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF, "the agent sees a close and retries on its own")
	assert.Equal(t, int64(1), sum(t, reader, "profiler_gateway_upstream_errors_total"))
}

// echoUpstream accepts connections and echoes every byte back.
func echoUpstream(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// TestReplayAndSpliceAreByteIdentical sends a handshake and a large random
// payload, half-closes, and expects the echo back in full. The handshake the
// gateway read must reach the upstream unchanged, both directions are
// spliced, and an end of stream is passed on, not turned into a reset.
func TestReplayAndSpliceAreByteIdentical(t *testing.T) {
	p := startProxy(t, baseConfig(echoUpstream(t)), nil)
	c, err := net.Dial("tcp", p.Addr().String())
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	data := append(handshakeBytes(t, "ns-1", "svc", "pod-1"), randomBytes(t, 4<<20)...)
	go func() {
		_, _ = c.Write(data)
		_ = c.(*net.TCPConn).CloseWrite()
	}()
	got, err := io.ReadAll(c)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got), "echo differs: got %d of %d bytes", len(got), len(data))
}

func TestMaxConnectionsRefuses(t *testing.T) {
	cfg := baseConfig(echoUpstream(t))
	cfg.MaxConnections = 1
	reader := sdkmetric.NewManualReader()
	p := startProxy(t, cfg, nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	// The handshake echo proves the first connection is spliced and so
	// holds the slot.
	first := dialEcho(t, p.Addr().String())
	defer func() { _ = first.Close() }()

	second, err := net.Dial("tcp", p.Addr().String())
	require.NoError(t, err)
	defer func() { _ = second.Close() }()
	require.NoError(t, second.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = second.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF, "over the cap the connection is closed on arrival")
	assert.Equal(t, int64(1), sum(t, reader, "profiler_gateway_connections_refused_total"))

	// The slot frees when the first connection ends.
	require.NoError(t, first.Close())
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", p.Addr().String())
		if err != nil {
			return false
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(time.Second))
		hs := handshakeBytes(t, "ns-1", "svc", "pod-2")
		if _, err := c.Write(hs); err != nil {
			return false
		}
		_, err = io.ReadFull(c, make([]byte, len(hs)))
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
}

func TestShutdownClosesAgentConnections(t *testing.T) {
	l := newRecordingListener()
	cfg := baseConfig(startCollector(t, l))
	p, err := New(cfg, defaultPolicy(), nopLogger(), noopMeter())
	require.NoError(t, err)
	require.NoError(t, p.Start(func(err error) { t.Errorf("accept failed: %v", err) }))

	ac := agent(t, p.Addr().String(), "ns-1", "svc", "pod-1")
	sendStream(t, ac, model.StreamTrace, []byte("data"))

	start := time.Now()
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Less(t, time.Since(start), 2*time.Second, "shutdown does not wait for long-lived agent connections")
	_, err = ac.InitStream(model.StreamTrace, 0, false)
	assert.Error(t, err, "the agent sees the close")
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.gone == 1
	}, 5*time.Second, 10*time.Millisecond, "the collector sees the disconnect")
}

func TestForwardedBytesAreCountedWhileOpen(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := startProxy(t, baseConfig(echoUpstream(t)), nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	c := dialEcho(t, p.Addr().String())
	defer func() { _ = c.Close() }()
	_, err := c.Write(make([]byte, 1000))
	require.NoError(t, err)
	_, err = io.ReadFull(c, make([]byte, 1000))
	require.NoError(t, err)

	hs := int64(len(handshakeBytes(t, "ns-1", "svc", "pod-1")))
	assert.Equal(t, hs+1000, sum(t, reader, "profiler_gateway_bytes_forwarded_total"))
	assert.Equal(t, int64(1), sum(t, reader, "profiler_gateway_connections_active"))
	assert.Equal(t, int64(1), sum(t, reader, "profiler_gateway_connections_total"))
}

func sum(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if s, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// BenchmarkRoundTrip measures the latency a gateway hop adds to a small
// request and its one-byte answer, the shape of RCV_DATA and its ack.
func BenchmarkRoundTrip(b *testing.B) {
	upstream := echoUpstream(b)
	for _, tc := range []struct {
		name string
		addr func() string
	}{
		{"direct", func() string { return upstream }},
		{"gateway", func() string {
			p, err := New(baseConfig(upstream), defaultPolicy(), nopLogger(), noopMeter())
			require.NoError(b, err)
			require.NoError(b, p.Start(func(error) {}))
			b.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			return p.Addr().String()
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := dialEcho(b, tc.addr())
			defer c.Close()
			msg := make([]byte, 1024)
			reply := make([]byte, 1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.Write(msg); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(c, reply); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIdleConnectionMemory reports heap and goroutine-stack bytes per
// open passthrough connection.
func BenchmarkIdleConnectionMemory(b *testing.B) {
	const n = 500
	upstream := echoUpstream(b)
	p, err := New(baseConfig(upstream), defaultPolicy(), nopLogger(), noopMeter())
	require.NoError(b, err)
	p.cfg.MaxConnections = n
	p.sem = make(chan struct{}, n)
	require.NoError(b, p.Start(func(error) {}))
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	for i := 0; i < b.N; i++ {
		before := memInUse()
		var conns []net.Conn
		for j := 0; j < n; j++ {
			conns = append(conns, dialEcho(b, p.Addr().String()))
		}
		after := memInUse()
		b.ReportMetric(float64(after-before)/n, "bytes/conn")
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

// memInUse counts the gateway side and the test's own client and echo
// sockets alike, so it overstates the gateway's share.
func memInUse() int64 {
	// Two cycles empty sync.Pool's victim cache, which would otherwise keep
	// the previous iteration's copy buffers in the baseline.
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse + m.StackInuse)
}
