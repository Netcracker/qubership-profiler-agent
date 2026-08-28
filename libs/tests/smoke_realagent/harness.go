//go:build smoke_realagent || smoke_realagent_v313

// harness.go holds what the HEAD-build variant (realagent_test.go) and the
// v3.1.3-download variant (realagent_v313_test.go) share: everything AFTER
// "the agent jar and test-app jar exist on disk" — running java -javaagent,
// waiting for the backend to be ready, and polling /api/v1/calls. Only "how
// do we obtain those two jars" differs (a Gradle build of HEAD vs a Maven
// Central download of a pinned old release), and that part stays in each
// test file.
//
// This file compiles under either build tag, so both test files can carry
// the same-named helpers without a duplicate-symbol error if someone ever
// runs both tags together (`-tags "smoke_realagent smoke_realagent_v313"`).
package smoke_realagent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	querymodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/require"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// repoRoot locates the repository root from this source file's own path, so
// the harness works regardless of the caller's working directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("REALAGENT_REPO_ROOT"); dir != "" {
		return dir
	}
	_, self, _, ok := runtime.Caller(0)
	require.True(t, ok, "cannot locate the test source to derive the repo root")
	// libs/tests/smoke_realagent → backend → repo root
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(self)))))
}

func splitHostPort(addr string) (string, string, error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("addr %q is not host:port", addr)
}

func waitReady(t *testing.T, u string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(u)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, timeout, 500*time.Millisecond, "service at %s must report READY", u)
}

// testWriter pipes a harness subprocess's output into the test log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[real-agent] %s", string(p))
	return len(p), nil
}

// javaAgentRun is the one thing every real-agent variant eventually does:
// run a test-app main class under -javaagent, pointed at a collector. Only
// how AgentJar/ProfilerHome/Classpath get produced differs between variants.
type javaAgentRun struct {
	AgentJar     string
	ProfilerHome string
	Config       string
	Host, Port   string
	Namespace    string
	Service      string
	Classpath    string
	MainClass    string
	// Args are extra CLI args appended after MainClass (e.g. the v3.1.3
	// test-app's Main takes an optional startup-delay-in-seconds arg;
	// AdversarialMain takes none).
	Args []string
}

// runJavaAgent runs one workload under -javaagent so it streams profiling
// data to run.Host:run.Port. The agent switches from local-file dumps to the
// TCP collector purely because REMOTE_DUMP_HOST is set (dumper/.../
// Dumper.java: remoteConfigured = isNotEmpty(REMOTE_DUMP_HOST)). The plain
// port defaults to ProtocolConst.PLAIN_SOCKET_PORT = 1715 when
// REMOTE_DUMP_PORT_SSL is unset, so REMOTE_DUMP_PORT_PLAIN is passed
// explicitly for clarity.
//
// -Dfile.encoding=UTF-8 keeps adversarial source literals intact on JVMs
// whose platform default is not UTF-8. The agent auto-detects its plugin
// jars from ${profiler.home}/lib; without an explicit profiler.home the
// agent would derive it as the grandparent of the config file (Bootstrap/
// DumpRootResolverAgent), which has no lib/ and fails to load plugins.
func runJavaAgent(t *testing.T, run javaAgentRun) {
	t.Helper()
	args := []string{
		"-Dfile.encoding=UTF-8",
		"-javaagent:" + run.AgentJar,
		"-Dprofiler.home=" + run.ProfilerHome,
		"-Dprofiler.config=" + run.Config,
		"-DREMOTE_DUMP_HOST=" + run.Host,
		"-DREMOTE_DUMP_PORT_PLAIN=" + run.Port,
		"-DCLOUD_NAMESPACE=" + run.Namespace,
		"-DMICROSERVICE_NAME=" + run.Service,
		"-cp", run.Classpath,
		run.MainClass,
	}
	args = append(args, run.Args...)
	t.Logf("running: java %s", strings.Join(args, " "))
	cmd := exec.Command("java", args...) //nolint:gosec // args are built from the harness's own resolved paths and fixed flags, not user input
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	require.NoError(t, cmd.Run(), "the workload must run cleanly under -javaagent")
}

// --- /api/v1/calls wire shape (subset) --------------------------------------

type callRow struct {
	PK         querymodel.PK       `json:"pk"`
	TsMs       int64               `json:"ts_ms"`
	DurationMs int32               `json:"duration_ms"`
	Method     string              `json:"method"`
	ThreadName string              `json:"thread_name"`
	Params     map[string][]string `json:"params"`
}

type callsPage struct {
	Calls          []callRow `json:"calls"`
	Partial        bool      `json:"partial"`
	PartialReasons []string  `json:"partial_reasons"`
}

// pollNamespaceCalls queries queryURL's /api/v1/calls for the window and
// keeps only the rows produced by our agent (matched by pod namespace and
// service — the pod name carries a runtime timestamp suffix), until at
// least `want` appear.
func pollNamespaceCalls(t *testing.T, queryURL, namespace, service string, fromMs, toMs int64, want int, timeout time.Duration) []callRow {
	t.Helper()
	var mine []callRow
	require.Eventually(t, func() bool {
		q := url.Values{}
		q.Set("from", fmt.Sprint(fromMs))
		q.Set("to", fmt.Sprint(toMs))
		q.Set("limit", "1000")
		resp, err := http.Get(queryURL + "/api/v1/calls?" + q.Encode())
		if err != nil {
			t.Logf("/calls: %s", err)
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Logf("/calls: %d %s", resp.StatusCode, body)
			return false
		}
		var page callsPage
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			t.Logf("/calls decode: %s", err)
			return false
		}
		mine = mine[:0]
		for _, r := range page.Calls {
			if r.PK.PodNamespace == namespace && r.PK.PodService == service {
				mine = append(mine, r)
			}
		}
		return len(mine) >= want
	}, timeout, 2*time.Second, "namespace %q service %q must show >= %d calls in [%d, %d]",
		namespace, service, want, fromMs, toMs)
	return mine
}
