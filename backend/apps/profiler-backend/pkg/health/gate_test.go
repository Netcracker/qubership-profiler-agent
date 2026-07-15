package health

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
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
