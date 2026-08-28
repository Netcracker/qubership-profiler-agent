package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestInstrumentHTTP pins the query HTTP histogram name: the load dashboards
// (load-testing-plan.md §6.2) read profiler_query_http_request_seconds.
func TestInstrumentHTTP(t *testing.T) {
	reg := NewRegistry()
	h := InstrumentHTTP(reg, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/pods", nil))

	families, err := reg.Gather()
	assert.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == "profiler_query_http_request_seconds" {
			return
		}
	}
	t.Fatal("profiler_query_http_request_seconds is not registered")
}

// TestMuxPprofGate pins the pprof exposure contract (load-testing-plan.md
// §6): the profile routes exist only when the flag is on, and turning them on
// never shadows /metrics or the API fallthrough.
func TestMuxPprofGate(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	for _, tc := range []struct {
		name      string
		withPprof bool
		wantPprof int
	}{
		{name: "disabled", withPprof: false, wantPprof: http.StatusTeapot},
		{name: "enabled", withPprof: true, wantPprof: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := Mux(NewRegistry(), next, tc.withPprof)

			get := func(path string) int {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				return rec.Code
			}

			// Off: /debug/pprof/ falls through to next like any other path.
			assert.Equal(t, tc.wantPprof, get("/debug/pprof/"))
			assert.Equal(t, tc.wantPprof, get("/debug/pprof/goroutine"))
			assert.Equal(t, http.StatusOK, get("/metrics"))
			assert.Equal(t, http.StatusTeapot, get("/api/v1/anything"))
		})
	}
}
