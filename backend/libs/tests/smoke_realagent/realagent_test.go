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
// The harness that builds and runs the Java agent lives at
// scripts/e2e-realagent/run-agent.sh; this test shells out to it.
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
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	querymodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

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
	cjk    = "語"     // 語  — one UTF-16 code unit >= 0x8000
	hangul = "한"     // 한  — one UTF-16 code unit >= 0x8000
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

// runAgent builds the real agent + the adversarial test-app and runs it under
// -javaagent so it streams to the collector at agentAddr.
func runAgent(t *testing.T) {
	t.Helper()
	root := repoRoot(t)
	script := filepath.Join(root, "scripts", "e2e-realagent", "run-agent.sh")
	require.FileExists(t, script, "harness script must exist")

	host, port, err := splitHostPort(agentAddr)
	require.NoError(t, err)

	cmd := exec.Command("bash", script)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"COLLECTOR_HOST="+host,
		"COLLECTOR_PORT="+port,
		"CLOUD_NAMESPACE="+namespace,
		"MICROSERVICE_NAME="+service,
	)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	require.NoError(t, cmd.Run(), "run-agent.sh must build and run the adversarial workload")
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
	rows := pollNamespaceCalls(t, fromMs, toMs, 2, 2*time.Minute)

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

// pollNamespaceCalls queries /api/v1/calls for the window and keeps only the
// rows produced by our agent (matched by pod namespace), until at least `want`
// appear.
func pollNamespaceCalls(t *testing.T, fromMs, toMs int64, want int, timeout time.Duration) []callRow {
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
	}, timeout, 2*time.Second, "namespace %q must show >= %d calls in [%d, %d]", namespace, want, fromMs, toMs)
	return mine
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

func splitHostPort(addr string) (string, string, error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("addr %q is not host:port", addr)
}

// testWriter pipes the harness output into the test log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[run-agent] %s", string(p))
	return len(p), nil
}
