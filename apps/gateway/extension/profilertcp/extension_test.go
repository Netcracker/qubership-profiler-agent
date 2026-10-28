package profilertcp

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/Netcracker/qubership-profiler-agent/libs/wire/common"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/emulator"
	profio "github.com/Netcracker/qubership-profiler-agent/libs/wire/io"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/server"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/units"
)

func loadConfig(t *testing.T, name string) (*Config, error) {
	t.Helper()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	sub, err := cm.Sub(name)
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig()
	return cfg.(*Config), sub.Unmarshal(cfg)
}

func TestDefaultConfig(t *testing.T) {
	cfg, err := loadConfig(t, "profiler_tcp")
	require.NoError(t, err)
	assert.Equal(t, createDefaultConfig(), cfg)
	assert.Equal(t, "0.0.0.0:1715", cfg.Endpoint)
	assert.Equal(t, 256, cfg.MaxConnections)
	assert.ErrorContains(t, cfg.Validate(), "upstream is required unless mode is discard")
}

func TestCustomConfig(t *testing.T) {
	cfg, err := loadConfig(t, "profiler_tcp/custom")
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.Equal(t, rules.Discard, cfg.Mode)
	assert.Equal(t, units.ByteSize(16<<10), cfg.CopyBufferSize)
	assert.Equal(t, 2*time.Second, cfg.ConnectTimeout)
	assert.Equal(t, 3*time.Second, cfg.HandshakeTimeout)
	assert.Equal(t, 10*time.Minute, cfg.Discard.RotationPeriod)
	assert.Equal(t, units.ByteSize(8<<20), cfg.Discard.RequiredRotationSize)
	assert.Equal(t, rules.Set{{Match: rules.Match{Service: "billing-*"}, Mode: rules.Passthrough}}, cfg.Rules)
}

func TestUnknownKeyIsRejected(t *testing.T) {
	_, err := loadConfig(t, "profiler_tcp/unknown_key")
	assert.ErrorContains(t, err, "max_conns")
}

func TestValidate(t *testing.T) {
	valid := func() *Config {
		c := createDefaultConfig()
		c.Upstream = "collector:1715"
		return c
	}
	require.NoError(t, valid().Validate())

	for name, mutate := range map[string]func(*Config){
		"bad mode":         func(c *Config) { c.Mode = "reject" },
		"upstream no port": func(c *Config) { c.Upstream = "collector" },
		"upstream url":     func(c *Config) { c.Upstream = "http://collector:1715/" },
		"bad endpoint":     func(c *Config) { c.Endpoint = "1715" },
		"zero timeout":     func(c *Config) { c.ConnectTimeout = 0 },
		"zero cap":         func(c *Config) { c.MaxConnections = 0 },
		"tiny buffer":      func(c *Config) { c.CopyBufferSize = 10 },
		"zero handshake":   func(c *Config) { c.HandshakeTimeout = 0 },
		"negative period":  func(c *Config) { c.Discard.RotationPeriod = -time.Second },
		"zero rotation":    func(c *Config) { c.Discard.RequiredRotationSize = 0 },
		"dump_type rule": func(c *Config) {
			c.Rules = rules.Set{{Match: rules.Match{DumpType: []string{"td"}}, Mode: rules.Discard}}
		},
	} {
		c := valid()
		mutate(c)
		assert.Error(t, c.Validate(), name)
	}
}

// podCounter is a libs/wire server.Listener that counts registered pods.
type podCounter struct {
	mu   sync.Mutex
	pods int
}

func (c *podCounter) RegisterPod(*server.ConnectedPod) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pods++
	return nil
}

func (c *podCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pods
}

func (*podCounter) AppendData(_ context.Context, _ *server.ConnectedPod, _ common.Uuid, chunk string) (int, error) {
	return len(chunk), nil
}

func (*podCounter) RegisterStream(context.Context, *server.ConnectedPod, common.Uuid, string, int, int, int, uint64, uint64) error {
	return nil
}
func (*podCounter) PodDisconnected(context.Context, *server.ConnectedPod)                {}
func (*podCounter) ReceivedCommand(context.Context, model.Command, time.Duration, error) {}
func (*podCounter) Read(context.Context, int, time.Duration, error)                      {}
func (*podCounter) Write(context.Context, int, time.Duration, error)                     {}
func (*podCounter) PrintDebug(context.Context)                                           {}
func (*podCounter) Close(context.Context)                                                {}

var timeouts = profio.TcpTimeout{
	ConnectTimeout: 2 * time.Second, SessionTimeout: time.Minute,
	ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
}

func connectAgent(t *testing.T, addr string) *emulator.AgentConnection {
	t.Helper()
	ac := emulator.PrepareAgent(context.Background(), nil, nil, "billing-1")
	require.NoError(t, ac.Prepare(emulator.ConnectionOpts{ProtocolAddress: addr, Timeout: timeouts}).Connect())
	t.Cleanup(func() { _ = ac.Close() })
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, "ns-1", "billing", "billing-1"))
	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)
	require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, []byte("data")))
	require.NoError(t, ac.Flush())
	return ac
}

func waitClosed(t *testing.T, ac *emulator.AgentConnection) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := ac.InitStream(model.StreamTrace, 0, false)
		return err != nil
	}, 5*time.Second, 20*time.Millisecond, "the rules change must close the connection")
}

// TestRulesFileMovesAConnectedAgent is the G4 reload acceptance test: rules
// file edits move a connected agent passthrough -> discard -> passthrough,
// with one reconnect per step and no restart of the extension.
func TestRulesFileMovesAConnectedAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collector := &podCounter{}
	ss := server.PrepareServer(ctx, server.ConnectionOpts{Timeout: timeouts}, collector)
	go func() { _ = ss.Start(ctx) }()
	t.Cleanup(ss.Stop)
	require.Eventually(t, func() bool { return ss.Addr() != nil }, 5*time.Second, 10*time.Millisecond)
	_, port, err := net.SplitHostPort(ss.Addr().String())
	require.NoError(t, err)

	rulesFile := filepath.Join(t.TempDir(), "rules.yaml")
	require.NoError(t, os.WriteFile(rulesFile, []byte("rules: []\n"), 0o600))
	cfg := createDefaultConfig()
	cfg.Endpoint = "127.0.0.1:0"
	cfg.Upstream = net.JoinHostPort("127.0.0.1", port)
	cfg.RulesFile = rulesFile
	require.NoError(t, cfg.Validate())
	ext, err := NewFactory().Create(context.Background(), extensiontest.NewNopSettings(componentType), cfg)
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, ext.Shutdown(context.Background())) })
	addr := ext.(*tcpExtension).proxy.Addr().String()

	ac := connectAgent(t, addr)
	require.Equal(t, 1, collector.count())

	require.NoError(t, os.WriteFile(rulesFile, []byte("rules:\n  - match: { service: billing }\n    mode: discard\n"), 0o600))
	waitClosed(t, ac)
	ac = connectAgent(t, addr)
	assert.Equal(t, 1, collector.count(), "the discarded reconnect never reaches the collector")

	require.NoError(t, os.WriteFile(rulesFile, []byte("rules: []\n"), 0o600))
	waitClosed(t, ac)
	connectAgent(t, addr)
	assert.Equal(t, 2, collector.count())
}

func TestStartFailsWhenThePortIsTaken(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	cfg := createDefaultConfig()
	cfg.Endpoint = ln.Addr().String()
	cfg.Mode = rules.Discard
	ext, err := NewFactory().Create(context.Background(), extensiontest.NewNopSettings(componentType), cfg)
	require.NoError(t, err)
	assert.Error(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	assert.NoError(t, ext.Shutdown(context.Background()))
}

func TestShutdownWithoutStart(t *testing.T) {
	ext, err := NewFactory().Create(context.Background(), extensiontest.NewNopSettings(componentType), createDefaultConfig())
	require.NoError(t, err)
	assert.NoError(t, ext.Shutdown(context.Background()))
}
