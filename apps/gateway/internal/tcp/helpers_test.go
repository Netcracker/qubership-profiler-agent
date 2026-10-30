package tcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/libs/wire/common"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/emulator"
	profio "github.com/Netcracker/qubership-profiler-agent/libs/wire/io"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/server"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

var testTimeout = profio.TcpTimeout{
	ConnectTimeout: 2 * time.Second,
	SessionTimeout: time.Minute,
	ReadTimeout:    5 * time.Second,
	WriteTimeout:   5 * time.Second,
}

// recordingListener is a server.Listener that keeps what the collector
// received, per pod and stream.
type recordingListener struct {
	mu      sync.Mutex
	pods    []server.ConnectedPod
	streams map[common.Uuid]string
	data    map[string][]byte // "pod/stream" -> bytes
	gone    int
}

func newRecordingListener() *recordingListener {
	return &recordingListener{streams: map[common.Uuid]string{}, data: map[string][]byte{}}
}

func (l *recordingListener) RegisterPod(pod *server.ConnectedPod) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pods = append(l.pods, *pod)
	return nil
}

func (l *recordingListener) AppendData(_ context.Context, pod *server.ConnectedPod, h common.Uuid, chunk string) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := pod.PodName + "/" + l.streams[h]
	l.data[key] = append(l.data[key], chunk...)
	return len(chunk), nil
}

func (l *recordingListener) RegisterStream(_ context.Context, _ *server.ConnectedPod, h common.Uuid, streamType string,
	_, _, _ int, _, _ uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.streams[h] = streamType
	return nil
}

func (l *recordingListener) PodDisconnected(context.Context, *server.ConnectedPod) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gone++
}

func (l *recordingListener) ReceivedCommand(context.Context, model.Command, time.Duration, error) {}
func (l *recordingListener) Read(context.Context, int, time.Duration, error)                      {}
func (l *recordingListener) Write(context.Context, int, time.Duration, error)                     {}
func (l *recordingListener) PrintDebug(context.Context)                                           {}
func (l *recordingListener) Close(context.Context)                                                {}

func (l *recordingListener) received(key string) []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.data[key]...)
}

func (l *recordingListener) podCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pods)
}

// startCollector runs a real libs/wire server on an ephemeral port.
func startCollector(t *testing.T, l server.Listener) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ss := server.PrepareServer(ctx, server.ConnectionOpts{ProtocolPort: 0, Timeout: testTimeout}, l)
	done := make(chan error, 1)
	go func() { done <- ss.Start(ctx) }()
	require.Eventually(t, func() bool { return ss.Addr() != nil }, 5*time.Second, 10*time.Millisecond)
	t.Cleanup(func() {
		ss.Stop()
		cancel()
		assert.NoError(t, <-done)
	})
	_, port, err := net.SplitHostPort(ss.Addr().String())
	require.NoError(t, err)
	return net.JoinHostPort("127.0.0.1", port)
}

func baseConfig(upstream string) Config {
	return Config{
		Endpoint:             "127.0.0.1:0",
		Upstream:             upstream,
		ConnectTimeout:       time.Second,
		MaxConnections:       16,
		CopyBufferSize:       32 << 10,
		HandshakeTimeout:     5 * time.Second,
		RequiredRotationSize: 4 << 20,
	}
}

func nopLogger() *zap.Logger { return zap.NewNop() }

func noopMeter() metric.Meter { return noop.NewMeterProvider().Meter("test") }

func defaultPolicy() *rules.Policy { return &rules.Policy{Default: rules.Passthrough} }

func startProxy(t *testing.T, cfg Config, policy *rules.Policy, provider ...*sdkmetric.MeterProvider) *Proxy {
	t.Helper()
	if policy == nil {
		policy = &rules.Policy{Default: rules.Passthrough}
	}
	meter := noop.NewMeterProvider().Meter("test")
	if len(provider) > 0 {
		meter = provider[0].Meter("test")
	}
	p, err := New(cfg, policy, zap.NewNop(), meter)
	require.NoError(t, err)
	require.NoError(t, p.Start(func(err error) { t.Errorf("accept failed: %v", err) }))
	t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.Background())) })
	return p
}

// agent connects a synthetic agent and completes the handshake.
func agent(t *testing.T, addr, namespace, service, pod string) *emulator.AgentConnection {
	t.Helper()
	ac := emulator.PrepareAgent(context.Background(), nil, nil, pod)
	require.NoError(t, ac.Prepare(emulator.ConnectionOpts{ProtocolAddress: addr, Timeout: testTimeout}).Connect())
	t.Cleanup(func() { _ = ac.Close() })
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, namespace, service, pod))
	return ac
}

// handshakeBytes encodes the agent's opening command, as the agent writes it.
func handshakeBytes(t testing.TB, namespace, service, pod string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := profio.PrepareTcpWriter(&buf)
	ctx := context.Background()
	require.NoError(t, w.WriteFixedByte(ctx, byte(model.COMMAND_GET_PROTOCOL_VERSION_V2)))
	require.NoError(t, w.WriteFixedLong(ctx, model.PROTOCOL_VERSION_V3))
	require.NoError(t, w.WriteFixedString(ctx, pod))
	require.NoError(t, w.WriteFixedString(ctx, service))
	require.NoError(t, w.WriteFixedString(ctx, namespace))
	require.NoError(t, w.Flush())
	return buf.Bytes()
}

// dialEcho opens a raw connection through the proxy to an echo upstream:
// it sends a handshake and reads its echo back.
func dialEcho(t testing.TB, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	hs := handshakeBytes(t, "ns-1", "svc", "pod-1")
	_, err = c.Write(hs)
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	got := make([]byte, len(hs))
	_, err = io.ReadFull(c, got)
	require.NoError(t, err)
	require.Equal(t, hs, got)
	require.NoError(t, c.SetReadDeadline(time.Time{}))
	return c
}

// sendStream opens a stream, sends the chunks as RCV_DATA, and flushes, so
// every ack has been drained when it returns.
func sendStream(t *testing.T, ac *emulator.AgentConnection, stream string, chunks ...[]byte) {
	t.Helper()
	reply, err := ac.InitStream(stream, 0, false)
	require.NoError(t, err)
	require.NotEqual(t, common.UUID{}, reply.Handle.Val, "the collector refused the stream")
	for _, c := range chunks {
		require.NoError(t, ac.CommandRcvData(stream, reply.Handle, c))
	}
	require.NoError(t, ac.Flush())
	require.Zero(t, ac.PendingAcks())
}
