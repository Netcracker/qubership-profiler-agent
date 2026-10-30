package profilerdumps

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/units"
)

const uploadPath = "/diagnostic/ns-1/2026/10/29/10/00/00/app-1_1761732000000/20261029T100000.td.txt"

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
	cfg, err := loadConfig(t, "profiler_dumps")
	require.NoError(t, err)
	assert.Equal(t, createDefaultConfig(), cfg)
	assert.Equal(t, units.ByteSize(10<<30), cfg.MaxBodySize)
	assert.Equal(t, 10*time.Minute, cfg.ResponseHeaderTimeout)
	assert.ErrorContains(t, cfg.Validate(), "upstream is required unless mode is discard")
}

func TestCustomConfig(t *testing.T) {
	cfg, err := loadConfig(t, "profiler_dumps/custom")
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.Equal(t, rules.Discard, cfg.Mode)
	assert.Equal(t, units.ByteSize(2<<30), cfg.MaxBodySize)
	assert.Equal(t, map[string]units.ByteSize{"hprof": 10 << 30, "td": 64 << 20, "gc": 1 << 20}, cfg.MaxBodySizePerType)
	assert.Equal(t, 15*time.Minute, cfg.ResponseHeaderTimeout)
	assert.Equal(t, time.Minute, cfg.DrainTimeout)
	require.Len(t, cfg.Rules, 1)
	assert.Equal(t, rules.Rule{
		Match: rules.Match{Namespace: "prod-*", DumpType: []string{"hprof"}},
		Mode:  rules.Passthrough,
	}, cfg.Rules[0])
}

func TestUnknownKeyIsRejected(t *testing.T) {
	_, err := loadConfig(t, "profiler_dumps/unknown_key")
	assert.ErrorContains(t, err, "upstreams")
}

func TestValidate(t *testing.T) {
	valid := func() *Config {
		c := createDefaultConfig()
		c.Upstream = "http://dumps:8080"
		return c
	}
	require.NoError(t, valid().Validate())

	discard := createDefaultConfig()
	discard.Mode = rules.Discard
	require.NoError(t, discard.Validate(), "discard needs no upstream")

	for name, mutate := range map[string]func(*Config){
		"bad mode":          func(c *Config) { c.Mode = "reject" },
		"upstream scheme":   func(c *Config) { c.Upstream = "dumps:8080" },
		"bad endpoint":      func(c *Config) { c.Endpoint = "8080" },
		"relative prefix":   func(c *Config) { c.PathPrefix = "diagnostic" },
		"no methods":        func(c *Config) { c.AllowedMethods = nil },
		"zero timeout":      func(c *Config) { c.ConnectTimeout = 0 },
		"zero body size":    func(c *Config) { c.MaxBodySize = 0 },
		"unknown dump type": func(c *Config) { c.MaxBodySizePerType = map[string]units.ByteSize{"core": 1} },
		"zero concurrency":  func(c *Config) { c.MaxConcurrentUploads = 0 },
		"service rule": func(c *Config) {
			c.Rules = rules.Set{{Match: rules.Match{Service: "x"}, Mode: rules.Discard}}
		},
		"passthrough rule without upstream": func(c *Config) {
			c.Upstream, c.Mode = "", rules.Discard
			c.Rules = rules.Set{{Mode: rules.Passthrough}}
		},
	} {
		c := valid()
		mutate(c)
		assert.Error(t, c.Validate(), name)
	}
}

// statusHost records the status events the extension reports.
type statusHost struct {
	component.Host
	mu     sync.Mutex
	events []componentstatus.Status
}

func (h *statusHost) Report(e *componentstatus.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, e.Status())
}

func (h *statusHost) statuses() []componentstatus.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]componentstatus.Status(nil), h.events...)
}

func startExtension(t *testing.T, cfg *Config, host component.Host) *dumpsExtension {
	t.Helper()
	require.NoError(t, cfg.Validate())
	ext, err := NewFactory().Create(context.Background(), extensiontest.NewNopSettings(componentType), cfg)
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), host))
	t.Cleanup(func() { assert.NoError(t, ext.Shutdown(context.Background())) })
	return ext.(*dumpsExtension)
}

func put(t *testing.T, base string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+uploadPath, bytes.NewReader([]byte("thread dump")))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestLifecycleAndHotReload(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(upstream.Close)

	rulesFile := filepath.Join(t.TempDir(), "rules.yaml")
	require.NoError(t, os.WriteFile(rulesFile, []byte("rules: []\n"), 0o600))

	cfg := createDefaultConfig()
	cfg.Endpoint = "127.0.0.1:0"
	cfg.Upstream = upstream.URL
	cfg.RulesFile = rulesFile
	host := &statusHost{Host: componenttest.NewNopHost()}
	ext := startExtension(t, cfg, host)
	base := "http://" + ext.srv.Addr().String()

	assert.Equal(t, http.StatusCreated, put(t, base))

	// A rules file edit switches the next upload, with no restart.
	require.NoError(t, os.WriteFile(rulesFile, []byte("rules:\n  - match: { dump_type: [td] }\n    mode: discard\n"), 0o600))
	require.Eventually(t, func() bool { return put(t, base) == http.StatusNoContent }, 5*time.Second, 20*time.Millisecond)

	// A broken file is rejected and reported; the discard rule stays.
	require.NoError(t, os.WriteFile(rulesFile, []byte("rules: ["), 0o600))
	require.Eventually(t, func() bool {
		return slices.Contains(host.statuses(), componentstatus.StatusRecoverableError)
	}, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, http.StatusNoContent, put(t, base))

	// Fixing it reports recovery.
	require.NoError(t, os.WriteFile(rulesFile, []byte("rules: []\n"), 0o600))
	require.Eventually(t, func() bool { return put(t, base) == http.StatusCreated }, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, componentstatus.StatusOK, host.statuses()[len(host.statuses())-1])
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
