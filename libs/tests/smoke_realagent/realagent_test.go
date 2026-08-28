//go:build smoke_realagent

// Package smoke_realagent is the REAL-agent counterpart of libs/tests/smoke:
// instead of a Go emulator synthesising agent bytes, it drives the actual Java
// profiler agent (built from this repo) against the running Go backend and
// asserts the adversarial method/param strings round-trip byte-exact.
//
// It is an acceptance gate for the decoder fixes. It currently FAILS on two
// backend decoder bugs (see the assertions below):
//
//   - Bug A — libs/parser/pipe/pipe_reader.go readChar reads a *signed* int16,
//     so every UTF-16 code unit >= U+8000 (CJK/Hangul) and both halves of a
//     non-BMP surrogate pair decode to U+FFFD.
//   - Bug B — libs/parser/pipe/dictionary.go skips an empty dictionary word
//     without advancing the id counter, so every later id shifts by one and
//     resolves to the wrong method/param name.
//
// The Go emulator never caught these because its own encoder
// (libs/tests/helpers/wire.putVarString) mirrors the buggy decoder: it writes
// len(runes) code units and cannot represent a non-BMP char or an empty phrase
// the way the real agent's DataOutputStreamEx.writeChars does.
//
// # Prerequisites
//
//   - A JDK (17+) and the repo's Gradle build (the harness builds the agent).
//   - The backend docker-compose stack up: collector on :1715, query on :8080.
//
// # Run
//
//	cd backend
//	docker compose up --build -d
//	go test -tags smoke_realagent -count=1 -timeout 20m -v ./libs/tests/smoke_realagent/...
//	docker compose down -v --remove-orphans
//
// or, in one shot:
//
//	make -C backend smoke-realagent
//
// Everything here is plain Go (os/exec driving gradlew/gradlew.bat, then
// java) — no shell script — so it runs the same way on Windows as on
// Linux/macOS, needing only a JDK 17+ and the repo's Gradle wrapper.
package smoke_realagent

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	querymodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	queryURL    = envOr("REALAGENT_QUERY_URL", "http://localhost:8080")
	internalURL = envOr("REALAGENT_INTERNAL_URL", "http://localhost:8081")
	agentAddr   = envOr("REALAGENT_AGENT_ADDR", "localhost:1715")
	// The namespace/service the harness tags the agent connection with. The pod
	// name carries a runtime timestamp suffix, so we match calls by namespace.
	namespace = envOr("REALAGENT_NAMESPACE", "e2e-realagent")
	service   = envOr("REALAGENT_SERVICE", "adversarial-app")
)

// --- Expected byte-exact strings (mirror of AdversarialMain.EXPECTED_*) ------
//
// Keep these in exact sync with
// test-app/src/main/java/com/netcracker/profilerTest/testapp/AdversarialMain.java.

const (
	cjk    = "語"          // 語  — one UTF-16 code unit >= 0x8000
	hangul = "한"          // 한  — one UTF-16 code unit >= 0x8000
	emoji  = "\U0001F525" // 🔥  — non-BMP, surrogate pair (both halves >= 0x8000)
	glyphs = cjk + hangul + emoji

	// Call A (bug A): Unicode on method name, param key and param value.
	expectMethodA     = "void com.acme.Svc." + glyphs + "_handle() (AdversarialMain.java) [test-app.jar]"
	expectParamKeyA   = "param." + glyphs
	expectParamValueA = "value-" + glyphs + "-tail"

	// Call B (bug B): plain ASCII, must survive the empty-word id shift.
	expectMethodB    = "void com.acme.Svc.plainAsciiHandleB() (AdversarialMain.java) [test-app.jar]"
	expectParamKeyB1 = "param.b.alpha"
	expectParamValB1 = "value-b-alpha"
	expectParamKeyB2 = "param.b.beta"
	expectParamValB2 = "value-b-beta"

	// Both adversarial calls sleep ~1200 ms; the agent's own housekeeping calls
	// are far shorter, so this threshold isolates the two calls we care about.
	minAdversarialDurationMs = 1000
)

// gradlewCommand builds the OS-appropriate invocation of the repo's Gradle
// wrapper. Windows can't exec a .bat file directly via CreateProcess the way
// Unix execs a script with a shebang, so route it through cmd.exe /C the same
// way a Windows shell would.
func gradlewCommand(root string, args ...string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		cmdArgs := append([]string{"/C", filepath.Join(root, "gradlew.bat")}, args...)
		return exec.Command("cmd", cmdArgs...) //nolint:gosec // args are fixed Gradle task names, not user input
	}
	return exec.Command(filepath.Join(root, "gradlew"), args...) //nolint:gosec // args are fixed Gradle task names, not user input
}

// resolveTestAppJar finds the newest qubership-profiler-test-app-*.jar under
// dir, skipping the -sources/-javadoc side jars. The jar carries a build
// version in its name, so it must be resolved by glob rather than a pinned
// path.
func resolveTestAppJar(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "qubership-profiler-test-app-*.jar"))
	if err != nil {
		return "", err
	}
	var candidates []string
	for _, m := range matches {
		if strings.HasSuffix(m, "-sources.jar") || strings.HasSuffix(m, "-javadoc.jar") {
			continue
		}
		candidates = append(candidates, m)
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no test-app jar found under %s", dir)
	}
	sort.Strings(candidates)
	return candidates[len(candidates)-1], nil
}

// buildHeadAgent builds the real agent + the adversarial test-app from
// whatever is checked out at HEAD (unless SKIP_BUILD=1 reuses an
// already-built profiler-home + jar) and returns the paths runJavaAgent needs.
func buildHeadAgent(t *testing.T) (agentJar, profilerHome, testAppJar string) {
	t.Helper()
	root := repoRoot(t)
	profilerHome = filepath.Join(root, "installer-zip-test", "build", "profiler-home")
	agentJar = filepath.Join(profilerHome, "lib", "qubership-profiler-agent.jar")

	if os.Getenv("SKIP_BUILD") != "1" {
		t.Logf("building the agent (installer zip) and the test-app jar...")
		// extractInstaller unpacks the installer zip into
		// installer-zip-test/build/profiler-home, which is exactly the
		// lib/ + config/ layout a deployed agent uses.
		cmd := gradlewCommand(root, "--quiet", ":installer-zip-test:extractInstaller", ":test-app:jar")
		cmd.Dir = root
		cmd.Stdout = testWriter{t}
		cmd.Stderr = testWriter{t}
		require.NoError(t, cmd.Run(), "gradlew must build the agent and the test-app")
	}

	require.FileExists(t, agentJar, "run without SKIP_BUILD=1, or build it first")

	jar, err := resolveTestAppJar(filepath.Join(root, "test-app", "build", "libs"))
	require.NoError(t, err, "test-app jar must exist under test-app/build/libs")
	testAppJar = jar
	return
}

// runAgent builds the real agent + the adversarial test-app and runs it under
// -javaagent so it streams to the collector at agentAddr.
func runAgent(t *testing.T) {
	t.Helper()
	agentJar, profilerHome, testAppJar := buildHeadAgent(t)

	host, port, err := splitHostPort(agentAddr)
	require.NoError(t, err)

	root := repoRoot(t)
	config := filepath.Join(root, "scripts", "e2e-realagent", "config", "_config.xml")

	runJavaAgent(t, javaAgentRun{
		AgentJar:     agentJar,
		ProfilerHome: profilerHome,
		Config:       config,
		Host:         host,
		Port:         port,
		Namespace:    namespace,
		Service:      service,
		Classpath:    testAppJar,
		MainClass:    "com.netcracker.profilerTest.testapp.AdversarialMain",
	})
}

func TestRealAgentAdversarialRoundTrip(t *testing.T) {
	waitReady(t, internalURL+"/internal/v1/health/ready", 2*time.Minute)
	waitReady(t, queryURL+"/api/v1/health/ready", time.Minute)

	// A stable now-anchored window that brackets the run. The agent stamps calls
	// with wall-clock ms, so a generous window keeps them in the hot tier.
	fromMs := time.Now().Add(-2 * time.Minute).UnixMilli()

	runAgent(t)

	toMs := time.Now().Add(2 * time.Minute).UnixMilli()

	// The agent flushes on graceful shutdown; give the collector a moment to
	// index, then poll until our namespace shows the two adversarial calls.
	rows := pollNamespaceCalls(t, queryURL, namespace, service, fromMs, toMs, 2, 2*time.Minute)

	// --- Locate the two adversarial calls. The /calls list rows do not carry
	// params (those live in /tree), and bug B corrupts Call B's *method name*,
	// so we cannot match Call B by name or param either. Both adversarial calls
	// sleep ~1200 ms, while the agent's own housekeeping calls are short — so we
	// take our namespace's two long calls and tell them apart by whether the
	// method still has the "_handle" shape (Call A only mangles its glyphs).
	var callA, callB *callRow
	for i := range rows {
		if rows[i].DurationMs < minAdversarialDurationMs {
			continue // skip the agent's short housekeeping calls
		}
		if indexOf(rows[i].Method, "_handle") >= 0 {
			callA = &rows[i]
		} else {
			callB = &rows[i]
		}
	}
	require.NotNil(t, callA, "Call A (the Unicode _handle call) must be present among %d namespace rows", len(rows))
	require.NotNil(t, callB, "Call B (the empty-word-shift call) must be present among %d namespace rows", len(rows))

	treeA := fetchTree(t, callA.PK)
	assertBugA(t, treeA)

	treeB := fetchTree(t, callB.PK)
	assertBugB(t, treeB)
}

// assertBugA pins the correct Unicode round-trip on Call A. It FAILS today:
// every code unit >= U+8000 comes back as U+FFFD.
func assertBugA(t *testing.T, tree *calltree.Tree) {
	t.Helper()
	require.NotNil(t, tree.Root)

	gotMethod := tree.Methods[tree.Root.MethodIdx]
	assert.Equal(t, expectMethodA, gotMethod,
		"BUG A: method name must round-trip byte-exact.\n"+
			"  expected: %q\n  actual:   %q\n"+
			"  (readChar reads a signed int16; every UTF-16 code unit >= U+8000 "+
			"and the emoji surrogate pair decode to U+FFFD)",
		expectMethodA, gotMethod)

	key, vals := firstParam(t, tree)
	assert.Equal(t, expectParamKeyA, key,
		"BUG A: param key must round-trip byte-exact.\n  expected: %q\n  actual:   %q",
		expectParamKeyA, key)
	require.NotEmpty(t, vals, "Call A must carry its param value")
	assert.Equal(t, expectParamValueA, vals[0],
		"BUG A: param value must round-trip byte-exact.\n  expected: %q\n  actual:   %q",
		expectParamValueA, vals[0])
}

// assertBugB pins the correct plain-ASCII round-trip on Call B. It FAILS today:
// the empty dictionary word shifts every later id by one, so the method name
// and param keys resolve to the wrong (neighbouring) dictionary word.
func assertBugB(t *testing.T, tree *calltree.Tree) {
	t.Helper()
	require.NotNil(t, tree.Root)

	gotMethod := tree.Methods[tree.Root.MethodIdx]
	assert.Equal(t, expectMethodB, gotMethod,
		"BUG B: method name must round-trip byte-exact.\n"+
			"  expected: %q\n  actual:   %q\n"+
			"  (an empty dictionary word is skipped without advancing the id "+
			"counter, so this method resolves to a shifted-by-one word)",
		expectMethodB, gotMethod)

	// Collect the resolved key -> values for the node.
	got := map[string][]string{}
	for _, p := range tree.Root.Params {
		key := tree.Params[p.ParamIdx]
		for _, g := range p.Groups {
			got[key] = append(got[key], g.Value)
		}
	}
	assert.Equal(t, []string{expectParamValB1}, got[expectParamKeyB1],
		"BUG B: param %q must resolve to its own value; the empty-word id shift "+
			"binds this value under a neighbouring key instead.\n  got map: %v",
		expectParamKeyB1, got)
	assert.Equal(t, []string{expectParamValB2}, got[expectParamKeyB2],
		"BUG B: param %q must resolve to its own value.\n  got map: %v",
		expectParamKeyB2, got)
}

// --- helpers -----------------------------------------------------------------

func firstParam(t *testing.T, tree *calltree.Tree) (string, []string) {
	t.Helper()
	require.NotEmpty(t, tree.Root.Params, "the call must carry at least one param")
	p := tree.Root.Params[0]
	key := tree.Params[p.ParamIdx]
	var vals []string
	for _, g := range p.Groups {
		vals = append(vals, g.Value)
	}
	return key, vals
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func fetchTree(t *testing.T, pk querymodel.PK) *calltree.Tree {
	t.Helper()
	u := queryURL + "/api/v1/calls/" + url.PathEscape(pk.PathString()) + "/tree"
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "/tree: %s", body)
	require.Equal(t, "application/x-msgpack", resp.Header.Get("Content-Type"))
	tree, _, err := calltree.Decode(body)
	require.NoError(t, err)
	return tree
}
