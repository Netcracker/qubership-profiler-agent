package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serve answers /api/v1/pods and /api/v1/calls from testdata, a call's
// /tree with sampleTree, and records the last query.
func serve(t *testing.T) (string, *http.Request) {
	t.Helper()
	last := &http.Request{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*last = *r.Clone(context.Background())
		if strings.HasSuffix(r.URL.Path, "/tree") {
			_, _ = w.Write(calltree.Encode(sampleTree()))
			return
		}
		name := map[string]string{"/api/v1/pods": "pods.json", "/api/v1/calls": "calls.json"}[r.URL.Path]
		if name == "" {
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, last
}

// sampleTree is a call tree whose node references follow the comments.
func sampleTree() *calltree.Tree {
	leaf := func(method int, ms int64) *calltree.Node {
		return &calltree.Node{MethodIdx: method, DurationMs: ms, SelfDurationMs: ms, Executions: 1, SelfExecutions: 1}
	}
	return &calltree.Tree{
		Methods: []string{"a.Main.run", "a.Repo.load", "a.Jdbc.execute"},
		Root: &calltree.Node{ // #0
			MethodIdx: 0, DurationMs: 1000, SelfDurationMs: 100, Executions: 5, SelfExecutions: 1,
			Children: []*calltree.Node{
				{ // #1
					MethodIdx: 1, DurationMs: 600, SelfDurationMs: 100, Executions: 2, SelfExecutions: 1,
					Children: []*calltree.Node{leaf(2, 500)}, // #2
				},
				{ // #3
					MethodIdx: 1, DurationMs: 300, SelfDurationMs: 50, Executions: 2, SelfExecutions: 1,
					Children: []*calltree.Node{leaf(2, 250)}, // #4
				},
			},
		},
	}
}

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	root := NewRoot(&out)
	root.SetArgs(args)
	root.SetErr(&bytes.Buffer{})
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

func TestCallsCommandSendsFlagsAsConditions(t *testing.T) {
	url, last := serve(t)
	out, err := run(t, "calls", "--url", url, "--since", "30m", "--service", "prod/billing",
		"--filter", "request_id=abc123", "--filter", "duration>1s", "--limit", "5")
	require.NoError(t, err)
	q := last.URL.Query()
	assert.Equal(t, []string{"request_id=abc123", "duration>1s", "namespace=prod", "service=billing"}, q["filter"])
	assert.Equal(t, "5", q.Get("limit"))
	assert.Contains(t, out, "request_id=abc123")

	// The printed cursor continues the listing under the same filters, and
	// the window of page 1 stays with the server's cursor.
	_, token, found := strings.Cut(out, "More calls: --cursor ")
	require.True(t, found, out)
	_, err = run(t, "calls", "--url", url, "--cursor", strings.TrimSpace(token))
	require.NoError(t, err)
	q = last.URL.Query()
	assert.Equal(t, "cursor-page-2", q.Get("cursor"))
	assert.ElementsMatch(t, []string{"request_id=abc123", "duration>1s", "namespace=prod", "service=billing"}, q["filter"])
	assert.Empty(t, q.Get("from"))
}

func TestJSONFlagPrintsTheTypedResult(t *testing.T) {
	url, _ := serve(t)
	out, err := run(t, "services", "--url", url, "--json")
	require.NoError(t, err)
	var res insight.ServicesResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	assert.Len(t, res.Namespaces, 2)
}

func TestURLComesFromTheEnvironment(t *testing.T) {
	url, _ := serve(t)
	t.Setenv(urlEnv, url)
	_, err := run(t, "services")
	assert.NoError(t, err)

	t.Setenv(urlEnv, "")
	_, err = run(t, "services")
	assert.ErrorContains(t, err, "set --url or PROFILER_URL")
}

func TestCommandErrors(t *testing.T) {
	url, _ := serve(t)
	for _, args := range [][]string{
		{"calls", "--url", url, "--since", "1h", "--from", "1760430000000"},
		{"calls", "--url", url, "--filter", "service"},
		{"explain", "--url", url},
		{"explain", "--url", url, "not-a-ref"},
		{"explain", "--url", url, "--node", "1", "--focus", "Repo", explainRef},
		{"explain", "--url", url, "--node", "-1", explainRef},
		{"explain", "--url", url, "--node", "5", explainRef},
	} {
		_, err := run(t, args...)
		assert.Error(t, err, "%v", args)
	}
}

const explainRef = "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error"

func TestExplainCommandFocusesOnANode(t *testing.T) {
	url, _ := serve(t)
	out, err := run(t, "explain", "--url", url, "--focus", "Repo.load", explainRef)
	require.NoError(t, err)
	assert.Contains(t, out, "Focus: #1 a.Repo.load\n", "the text focus takes the slower match")
	assert.Contains(t, out, "To explain another, focus on its node: 3 (300 ms)")

	out, err = run(t, "explain", "--url", url, "--node", "3", explainRef)
	require.NoError(t, err)
	assert.Contains(t, out, "Focus: #3 a.Repo.load\n")
	assert.Contains(t, out, "\n#3 a.Repo.load  300, 50, x1\n  #4 a.Jdbc.execute  250, 250, x1\n")

	out, err = run(t, "explain", "--url", url, "--node", "3", "--json", explainRef)
	require.NoError(t, err)
	var res insight.ExplainResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	assert.Equal(t, 3, res.FocusNode)
	assert.Equal(t, 3, res.HotPath.Node)
	assert.Contains(t, out, `"focus_node": 3`)
}
