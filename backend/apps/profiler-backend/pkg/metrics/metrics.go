// Package metrics wires the profiler-backend Prometheus series over the
// snapshot seams the libs expose. Metric names are a stable contract for
// dashboards and alerts (see charts/profiler-backend/README.md); labels stay
// low-cardinality — retention_class, truncated_reason, kind, layer, result —
// never pod, PK, or replica.
package metrics

import (
	"net/http"

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
// API routes still answer 503 behind the health gate.
func Mux(reg *prometheus.Registry, next http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	mux.Handle("/", next)
	return mux
}
