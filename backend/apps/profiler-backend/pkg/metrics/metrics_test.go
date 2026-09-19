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

// TestMetricsMux pins the dedicated metrics listener: /metrics serves the
// registry, pprof is gated by the flag, and nothing else falls through — an
// unmatched path answers 404 rather than an API or UI route.
func TestMetricsMux(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withPprof bool
		wantPprof int
	}{
		{name: "disabled", withPprof: false, wantPprof: http.StatusNotFound},
		{name: "enabled", withPprof: true, wantPprof: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := MetricsMux(NewRegistry(), tc.withPprof)

			get := func(path string) (int, string) {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				return rec.Code, rec.Body.String()
			}

			code, body := get("/metrics")
			assert.Equal(t, http.StatusOK, code)
			assert.Contains(t, body, "# HELP", "/metrics must serve Prometheus exposition")

			pprofCode, _ := get("/debug/pprof/")
			assert.Equal(t, tc.wantPprof, pprofCode)

			// No fallthrough: an API path is a plain 404, never the registry.
			apiCode, _ := get("/api/v1/anything")
			assert.Equal(t, http.StatusNotFound, apiCode)
		})
	}
}

// TestQueryHandlersSplit pins the security contract of the reports2#15 fix: the
// external surface never exposes /metrics or /debug/pprof, so the ingress that
// maps the external port at path / cannot reach them (04 §12). Only the metrics
// listener serves them.
func TestQueryHandlersSplit(t *testing.T) {
	reg := NewRegistry()
	// Stand-in for the gated /api/v1 + UI handler: any non-metrics path lands here.
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	external, metricsH := QueryHandlers(reg, api, true)

	get := func(h http.Handler, path string) (int, string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code, rec.Body.String()
	}

	// External: /metrics and /debug/pprof fall through to the API stand-in, so
	// they never carry the Prometheus exposition or the pprof index. Re-adding
	// Mux to this listener would break both assertions.
	_, extMetrics := get(external, "/metrics")
	assert.NotContains(t, extMetrics, "# HELP", "external port must not serve Prometheus exposition")
	extPprofCode, extPprof := get(external, "/debug/pprof/")
	assert.Equal(t, http.StatusTeapot, extPprofCode)
	assert.NotContains(t, extPprof, "Types of profiles available", "external port must not serve pprof")

	// Metrics listener: both surfaces live here.
	metricsCode, metricsBody := get(metricsH, "/metrics")
	assert.Equal(t, http.StatusOK, metricsCode)
	assert.Contains(t, metricsBody, "# HELP")
	pprofCode, _ := get(metricsH, "/debug/pprof/")
	assert.Equal(t, http.StatusOK, pprofCode)
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
