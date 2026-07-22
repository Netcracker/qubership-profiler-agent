// Package metrics wires the profiler-backend Prometheus series over the
// snapshot seams the libs expose. Metric names are a stable contract for
// dashboards and alerts (see charts/profiler-backend/README.md); labels stay
// low-cardinality — retention_class, truncated_reason, kind, layer, result —
// never pod, PK, or replica.
package metrics

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "profiler"

// NewRegistry returns a fresh registry preloaded with the Go runtime and
// process collectors. Each subcommand owns one registry; nothing registers on
// the global default.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves the registry in the Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Mux routes /metrics to the registry and everything else to next. The mux
// binds before recovery, so a scrape works through LOADING/RECOVERY while the
// API routes still answer 503 behind the health gate. withPprof additionally
// mounts the net/http/pprof handlers for load tests and incident debugging
// (load-testing-plan.md §6); they ride the same port as /metrics, outside the
// health gate, so a profile can be taken mid-recovery.
func Mux(reg *prometheus.Registry, next http.Handler, withPprof bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	if withPprof {
		RegisterPprof(mux)
	}
	mux.Handle("/", next)
	return mux
}

// InstrumentHTTP wraps next with the profiler_query_http_request_seconds
// histogram (code, method). No path label: call PKs in paths would explode
// the cardinality, and the load dashboards (load-testing-plan.md §6.2) need
// rates and latency percentiles, not per-route breakdowns.
func InstrumentHTTP(reg prometheus.Registerer, next http.Handler) http.Handler {
	seconds := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "query",
		Name:    "http_request_seconds",
		Help:    "Duration of one external API request (/api/v1 and the UI assets).",
		Buckets: prometheus.DefBuckets,
	}, []string{"code", "method"})
	reg.MustRegister(seconds)
	return promhttp.InstrumentHandlerDuration(seconds, next)
}

// RegisterPprof mounts the net/http/pprof handlers under /debug/pprof/.
// Importing net/http/pprof registers them only on http.DefaultServeMux, which
// no subcommand serves; this explicit registration is the sole route to the
// profiles. Index dispatches the named profiles (heap, goroutine, block, ...)
// below /debug/pprof/ by itself; the other four need their own routes.
func RegisterPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
