//go:build smoke_realagent_v313

// Package smoke_realagent is the regression gate for the "gc" stream
// compatibility fix (01-write-contract.md §1, 06-wire-protocol-server.md
// §4/§8/§9.4): agents built before v3.1.4 register an eighth "gc" stream
// unconditionally whenever they stream directly to a collector, regardless
// of whether GC-log harvesting is even enabled. Before the fix the
// collector treated "gc" as an unknown stream and tore the WHOLE connection
// down on it (backend/libs/server/server_connection.go's CommandInitStream),
// so a pre-v3.1.4 agent wrote no data at all — not just its GC-log bytes.
//
// This test drives the ACTUAL v3.1.3 Java agent against the running Go
// backend and asserts its calls land in /api/v1/calls. On an unfixed backend
// this test fails (zero rows, the connection never got past the "gc"
// INIT_STREAM_V2); after the fix it passes.
//
// Unlike the byte-exactness smoke_realagent suite (realagent_test.go, which
// builds the agent from whatever is checked out at HEAD via a Gradle build),
// this test fetches the pre-built v3.1.3 release straight from Maven Central
// — v3.1.4 deleted GCDumper and the "gc" stream entirely, so HEAD can no
// longer reproduce the bug, and a full old-tag Gradle build is both slow and
// unnecessary when the release is already published:
//
//   - org.qubership.profiler:qubership-profiler-installer:3.1.3 (zip) — the
//     exact lib/ + config/ layout a local Gradle build's extractInstaller
//     task produces, including the shaded lib/qubership-profiler-runtime.jar
//     that actually carries Dumper/GCDumper. (qubership-profiler-runtime's
//     own plain Maven Central jar is a near-empty aggregator: the runtime
//     module has no sources of its own, just a shadowJar task merging
//     dumper + instrumenter, and only the plain unshaded jar gets
//     published — the functional fat jar only exists inside the installer
//     distribution zip.)
//   - org.qubership.profiler:qubership-profiler-test-app:3.1.3 (jar) — the
//     plain Main class this tag's test-app ships (it predates the
//     programmatic-Profiler-API AdversarialMain used by realagent_test.go).
//
// Everything here is plain Go (net/http, archive/zip, crypto/sha1,
// os/exec via the shared harness.go) — no shell script, so this test runs
// the same way on Windows as on Linux/macOS, needing only a JRE on PATH to
// run the downloaded agent.
//
// # Prerequisites
//
//   - A JRE (java on PATH) to run the downloaded agent — no JDK or Gradle
//     build needed for this variant.
//   - The backend docker-compose stack up: collector on :1715, query on :8080.
//
// # Run
//
//	cd backend
//	docker compose up --build -d
//	go test -tags smoke_realagent_v313 -count=1 -timeout 10m -v ./libs/tests/smoke_realagent/...
//	docker compose down -v --remove-orphans
//
// or, in one shot:
//
//	make -C backend smoke-realagent-v313
package smoke_realagent

import (
	"archive/zip"
	"crypto/sha1" //nolint:gosec // Maven Central publishes checksums as SHA-1, not chosen for cryptographic strength
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	v313QueryURL    = envOr("REALAGENT_V313_QUERY_URL", "http://localhost:8080")
	v313InternalURL = envOr("REALAGENT_V313_INTERNAL_URL", "http://localhost:8081")
	v313AgentAddr   = envOr("REALAGENT_V313_AGENT_ADDR", "localhost:1715")
	// The pod name carries a runtime timestamp suffix, so calls are matched
	// by namespace/service instead.
	v313Namespace = envOr("REALAGENT_V313_NAMESPACE", "e2e-realagent-v313")
	v313Service   = envOr("REALAGENT_V313_SERVICE", "legacy-gc-app")

	v313AgentVersion = envOr("AGENT_VERSION", "3.1.3")
	v313MavenRepoURL = envOr("MAVEN_REPO_URL", "https://repo1.maven.org/maven2")
)

const v313GroupPath = "org/qubership/profiler"

// fetchV313Agent downloads the real v3.1.3 agent and test-app (pre-built,
// from Maven Central) into a build-local cache dir and returns the paths
// runJavaAgent needs.
func fetchV313Agent(t *testing.T) (agentJar, profilerHome, testAppJar string) {
	t.Helper()
	root := repoRoot(t)
	downloadDir := filepath.Join(root, "installer-zip-test", "build", "v313-download")
	profilerHome = filepath.Join(downloadDir, "profiler-home")
	agentJar = filepath.Join(profilerHome, "lib", "qubership-profiler-agent.jar")
	testAppJar = filepath.Join(downloadDir, fmt.Sprintf("qubership-profiler-test-app-%s.jar", v313AgentVersion))

	if os.Getenv("SKIP_DOWNLOAD") != "1" {
		require.NoError(t, os.RemoveAll(downloadDir))
		require.NoError(t, os.MkdirAll(profilerHome, 0o755))

		installerZip := filepath.Join(downloadDir, fmt.Sprintf("qubership-profiler-installer-%s.zip", v313AgentVersion))
		installerURL := fmt.Sprintf("%s/%s/qubership-profiler-installer/%s/qubership-profiler-installer-%s.zip",
			v313MavenRepoURL, v313GroupPath, v313AgentVersion, v313AgentVersion)
		fetchAndVerify(t, installerURL, installerZip)
		unzipTo(t, installerZip, profilerHome)

		testAppURL := fmt.Sprintf("%s/%s/qubership-profiler-test-app/%s/qubership-profiler-test-app-%s.jar",
			v313MavenRepoURL, v313GroupPath, v313AgentVersion, v313AgentVersion)
		fetchAndVerify(t, testAppURL, testAppJar)
	}

	require.FileExists(t, agentJar, "run without SKIP_DOWNLOAD=1, or fetch it first")
	require.FileExists(t, testAppJar, "run without SKIP_DOWNLOAD=1, or fetch it first")
	return
}

// runV313Agent fetches the real v3.1.3 agent and test-app and runs the
// latter under -javaagent so it streams to the collector at v313AgentAddr.
func runV313Agent(t *testing.T) {
	t.Helper()
	agentJar, profilerHome, testAppJar := fetchV313Agent(t)

	host, port, err := splitHostPort(v313AgentAddr)
	require.NoError(t, err)

	root := repoRoot(t)
	config := filepath.Join(root, "scripts", "e2e-realagent", "config", "_config-v313.xml")

	// The v3.1.3 test-app's Main class exercises Main.test() through
	// ordinary bytecode instrumentation (it predates the
	// programmatic-Profiler-API AdversarialMain), so _config-v313.xml turns
	// profiling ON for its package instead of excluding it.
	runJavaAgent(t, javaAgentRun{
		AgentJar:     agentJar,
		ProfilerHome: profilerHome,
		Config:       config,
		Host:         host,
		Port:         port,
		Namespace:    v313Namespace,
		Service:      v313Service,
		Classpath:    testAppJar,
		MainClass:    "com.netcracker.profilerTest.testapp.Main",
		Args:         []string{"5"},
	})
}

// fetchAndVerify downloads url into dest and checks it against Maven
// Central's published SHA-1 sidecar (url+".sha1"), so a truncated or
// tampered download fails loudly here instead of surfacing as a confusing
// java/zip error later.
func fetchAndVerify(t *testing.T, url, dest string) {
	t.Helper()
	t.Logf("fetching %s", url)
	fetchFile(t, url, dest)
	want := strings.TrimSpace(fetchString(t, url+".sha1"))
	got := sha1File(t, dest)
	require.Equal(t, want, got, "SHA-1 mismatch for %s", url)
}

func fetchFile(t *testing.T, u, dest string) {
	t.Helper()
	resp, err := http.Get(u) //nolint:gosec // u is built from a fixed Maven Central base + version, not user input
	require.NoError(t, err, "GET %s", u)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", u)
	f, err := os.Create(dest) //nolint:gosec // dest is built from filepath.Join under our own build dir
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, resp.Body)
	require.NoError(t, err, "writing %s", dest)
}

func fetchString(t *testing.T, u string) string {
	t.Helper()
	resp, err := http.Get(u) //nolint:gosec // u is built from a fixed Maven Central base + version, not user input
	require.NoError(t, err, "GET %s", u)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", u)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func sha1File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // path is built from filepath.Join under our own build dir
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	h := sha1.New() //nolint:gosec // matching Maven Central's own checksum algorithm, not used for security
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

// unzipTo extracts a zip archive into dir, matching the lib/ + config/
// layout the installer distribution ships (the same shape a local Gradle
// build's extractInstaller task produces for HEAD).
func unzipTo(t *testing.T, zipPath, dir string) {
	t.Helper()
	r, err := zip.OpenReader(zipPath)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	cleanDir := filepath.Clean(dir) + string(os.PathSeparator)
	for _, f := range r.File {
		// Reject a zip entry that would extract outside dir ("zip slip");
		// the archive comes from a checksum-verified Maven Central download,
		// but there is no reason to trust its internal paths blindly.
		destPath := filepath.Join(dir, f.Name)
		require.True(t, strings.HasPrefix(destPath+string(os.PathSeparator), cleanDir) || destPath == filepath.Clean(dir),
			"zip entry %q escapes %s", f.Name, dir)

		if f.FileInfo().IsDir() {
			require.NoError(t, os.MkdirAll(destPath, 0o755))
			continue
		}
		require.NoError(t, os.MkdirAll(filepath.Dir(destPath), 0o755))
		extractOne(t, f, destPath)
	}
}

func extractOne(t *testing.T, f *zip.File, destPath string) {
	t.Helper()
	rc, err := f.Open()
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode()) //nolint:gosec // destPath is validated by unzipTo
	require.NoError(t, err)
	defer func() { _ = out.Close() }()
	_, err = io.Copy(out, rc) //nolint:gosec // the archive is checksum-verified before extraction, size is bounded by the release artifact
	require.NoError(t, err)
}

func TestRealAgentV313WritesData(t *testing.T) {
	waitReady(t, v313InternalURL+"/internal/v1/health/ready", 2*time.Minute)
	waitReady(t, v313QueryURL+"/api/v1/health/ready", time.Minute)

	// A stable now-anchored window that brackets the run. The agent stamps
	// calls with wall-clock ms, so a generous window keeps them in the hot
	// tier.
	fromMs := time.Now().Add(-2 * time.Minute).UnixMilli()

	runV313Agent(t)

	toMs := time.Now().Add(2 * time.Minute).UnixMilli()

	// The agent flushes on graceful shutdown; give the collector a moment to
	// index, then poll until our namespace/service shows at least one call.
	// Before the "gc" stream fix this never happens: the v3.1.3 agent's very
	// first INIT_STREAM_V2 (for "gc") tore the connection down before any
	// real data stream had a chance. pollNamespaceCalls itself fails the test
	// if the timeout elapses with no matching call.
	pollNamespaceCalls(t, v313QueryURL, v313Namespace, v313Service, fromMs, toMs, 1, 2*time.Minute)
}
