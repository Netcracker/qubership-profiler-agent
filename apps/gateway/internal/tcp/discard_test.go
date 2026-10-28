package tcp

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Netcracker/qubership-profiler-agent/libs/wire/emulator"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// TestDiscardSessionCompletesWithoutTheUpstream is the G4 acceptance test:
// a discarded agent completes the handshake, opens streams, sends data, and
// flushes with every ack drained and no ACK_ERROR_MAGIC, while nothing
// reaches the upstream.
func TestDiscardSessionCompletesWithoutTheUpstream(t *testing.T) {
	l := newRecordingListener()
	cfg := baseConfig(startCollector(t, l))
	cfg.RotationPeriod = 5 * time.Minute
	reader := sdkmetric.NewManualReader()
	p := startProxy(t, cfg, &rules.Policy{Default: rules.Discard}, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	ac := agent(t, p.Addr().String(), "ns-1", "svc", "pod-1")
	assert.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())

	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(4<<20), reply.RequiredRotationSize)
	assert.Equal(t, uint64(5*time.Minute/time.Millisecond), reply.RotationPeriodMs)
	for i := 0; i < 20; i++ {
		require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, randomBytes(t, 1024)))
	}
	require.NoError(t, ac.Flush(), "every ack drains; ACK_ERROR_MAGIC would fail the flush")
	assert.Zero(t, ac.PendingAcks())
	sendStream(t, ac, model.StreamCalls, []byte("calls"))
	require.NoError(t, ac.CommandClose())

	assert.Zero(t, l.podCount(), "nothing reaches the upstream")
	assert.Greater(t, sum(t, reader, "profiler_gateway_bytes_discarded_total"), int64(20*1024))
	assert.Equal(t, int64(1), sum(t, reader, "profiler_gateway_connections_total"))
}

func TestDiscardWorksWithoutUpstream(t *testing.T) {
	p := startProxy(t, baseConfig(""), &rules.Policy{Default: rules.Discard})
	ac := agent(t, p.Addr().String(), "ns-1", "svc", "pod-1")
	sendStream(t, ac, model.StreamTrace, []byte("dropped"))
}

func TestRulesSelectModeFromTheHandshake(t *testing.T) {
	l := newRecordingListener()
	p := startProxy(t, baseConfig(startCollector(t, l)), &rules.Policy{
		Default: rules.Passthrough,
		Inline: rules.Set{
			{Match: rules.Match{Service: "chatty-*"}, Mode: rules.Discard},
			{Match: rules.Match{Namespace: "load-*"}, Mode: rules.Discard},
		},
	})

	for _, a := range []struct{ ns, svc, pod string }{
		{"ns-1", "chatty-api", "chatty-api-1"},
		{"load-7", "billing", "billing-load"},
		{"ns-1", "billing", "billing-1"},
	} {
		sendStream(t, agent(t, p.Addr().String(), a.ns, a.svc, a.pod), model.StreamTrace, []byte(a.pod))
	}
	require.Eventually(t, func() bool { return l.podCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, "billing-1", l.pods[0].PodName)
	assert.Equal(t, []byte("billing-1"), l.received("billing-1/"+model.StreamTrace))
}

// TestReloadMovesAConnectedAgent: a rules change closes the connections
// whose mode it changes, and the reconnecting agent lands in the new mode.
// It moves an agent passthrough → discard → passthrough, one reconnect per
// step.
func TestReloadMovesAConnectedAgent(t *testing.T) {
	l := newRecordingListener()
	policy := &rules.Policy{Default: rules.Passthrough}
	reader := sdkmetric.NewManualReader()
	p := startProxy(t, baseConfig(startCollector(t, l)), policy, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	other := agent(t, p.Addr().String(), "ns-1", "untouched", "untouched-1")

	ac := agent(t, p.Addr().String(), "ns-1", "billing", "billing-1")
	sendStream(t, ac, model.StreamTrace, []byte("one"))
	require.Equal(t, 2, l.podCount())

	policy.SetFileRules(rules.Set{{Match: rules.Match{Service: "billing"}, Mode: rules.Discard}})
	p.Reevaluate()
	requireClosed(t, ac)
	ac = agent(t, p.Addr().String(), "ns-1", "billing", "billing-1")
	sendStream(t, ac, model.StreamTrace, []byte("two"))
	assert.Equal(t, 2, l.podCount(), "the discarded reconnect never reaches the upstream")

	policy.SetFileRules(nil)
	p.Reevaluate()
	requireClosed(t, ac)
	ac = agent(t, p.Addr().String(), "ns-1", "billing", "billing-1")
	sendStream(t, ac, model.StreamTrace, []byte("three"))
	require.Eventually(t, func() bool { return l.podCount() == 3 }, 5*time.Second, 10*time.Millisecond)

	sendStream(t, other, model.StreamTrace, []byte("still here"))
	assert.Equal(t, int64(2), sum(t, reader, "profiler_gateway_connections_closed_by_reload_total"))
}

// requireClosed waits until the agent's connection is gone.
func requireClosed(t *testing.T, ac *emulator.AgentConnection) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := ac.InitStream(model.StreamTrace, 0, false)
		return err != nil
	}, 5*time.Second, 20*time.Millisecond, "the connection must be closed")
}

func TestBadHandshakeIsClosed(t *testing.T) {
	cfg := baseConfig(echoUpstream(t))
	cfg.HandshakeTimeout = 200 * time.Millisecond
	p := startProxy(t, cfg, nil)

	for name, payload := range map[string][]byte{
		"legacy handshake": {0x08, 0, 0, 0, 0},
		"truncated":        handshakeBytes(t, "ns-1", "svc", "pod-1")[:12],
		"silent":           nil,
	} {
		c, err := net.Dial("tcp", p.Addr().String())
		require.NoError(t, err, name)
		_, err = c.Write(payload)
		require.NoError(t, err, name)
		require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, err = c.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.EOF, name)
		_ = c.Close()
	}
}
