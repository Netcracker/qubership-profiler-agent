package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func get(t *testing.T, g *Gate, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestGateLifecycle(t *testing.T) {
	g := NewGate("/internal/v1")

	rec := get(t, g, "/internal/v1/health/ready")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"state":"INIT"}`, rec.Body.String())
	assert.Equal(t, http.StatusOK, get(t, g, "/internal/v1/health/live").Code,
		"liveness holds through startup: only FATAL may fail it")

	g.Set(StateRecovery, "replaying WALs")
	rec = get(t, g, "/internal/v1/health/ready")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"state":"RECOVERY","details":"replaying WALs"}`, rec.Body.String())
	assert.Equal(t, http.StatusOK, get(t, g, "/internal/v1/health/live").Code,
		"liveness is recovery-independent: a long recovery must never earn a kubelet kill, "+
			"or the pod loops LOADING→kill→LOADING forever (03 §4)")
	assert.Equal(t, http.StatusServiceUnavailable, get(t, g, "/internal/v1/calls").Code,
		"API routes answer 503 until the handler is mounted")

	g.Mount(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	g.Set(StateReady, "")
	assert.Equal(t, http.StatusOK, get(t, g, "/internal/v1/health/ready").Code)
	assert.Equal(t, http.StatusTeapot, get(t, g, "/internal/v1/calls").Code)

	g.Set(StateDraining, "received SIGTERM")
	assert.Equal(t, http.StatusServiceUnavailable, get(t, g, "/internal/v1/health/ready").Code)
	assert.Equal(t, http.StatusOK, get(t, g, "/internal/v1/health/live").Code)
	assert.Equal(t, http.StatusTeapot, get(t, g, "/internal/v1/calls").Code,
		"DRAINING keeps serving in-flight traffic (03 §5.1)")

	g.Set(StateFatal, "corrupt sqlite")
	assert.Equal(t, http.StatusServiceUnavailable, get(t, g, "/internal/v1/health/live").Code)
}

// While the API handler is unmounted the gate is the API's error surface, so
// every non-probe route — /api/v1 and the SPA shell alike — answers the 02 §8
// envelope rather than the probe's state body. The probes keep that body: it
// is pinned by 03-lifecycle.md §4 and read by a human in kubelet logs, not by
// an API client.
func TestGateNotReadyAnswersTheProblemEnvelope(t *testing.T) {
	g := NewGate("/api/v1")
	g.Set(StateLoading, "opening the object store")

	for _, path := range []string{"/api/v1/calls", "/"} {
		rec := get(t, g, path)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, path)
		assert.Equal(t, httpproblem.ContentType, rec.Header().Get("Content-Type"), path)

		var body struct {
			Type   string `json:"type"`
			Title  string `json:"title"`
			Status int    `json:"status"`
			Detail string `json:"detail"`
			Code   string `json:"code"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "path %s body: %s", path, rec.Body)
		assert.Equal(t, "about:blank", body.Type, path)
		assert.Equal(t, http.StatusServiceUnavailable, body.Status, path)
		assert.Equal(t, httpproblem.CodeNotReady, body.Code, path)
		assert.Contains(t, body.Detail, string(StateLoading), path)
		assert.Contains(t, body.Detail, "opening the object store", path)
	}

	rec := get(t, g, "/api/v1/health/ready")
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"state":"LOADING","details":"opening the object store"}`, rec.Body.String(),
		"the probe body is what a human reads in kubelet logs (03 §4)")
}
