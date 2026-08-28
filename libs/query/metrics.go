package query

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics carries the query-service Prometheus series. The metric names are a
// stable contract for dashboards and alerts (see the chart README); labels
// stay low-cardinality — no replica, pod, or PK labels. Every method is
// nil-receiver-safe so tests and embedders run without a registry.
type Metrics struct {
	fanoutSeconds    *prometheus.HistogramVec
	coldLists        prometheus.Counter
	partialResponses prometheus.Counter
	guardRejections  *prometheus.CounterVec
	budgetLimit      prometheus.Gauge
	budgetUsed       prometheus.Gauge
	budgetDenials    *prometheus.CounterVec
	budgetOverruns   prometheus.Counter
}

// NewMetrics registers the query series on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		fanoutSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "profiler", Subsystem: "query",
			Name:    "fanout_replica_request_seconds",
			Help:    "Duration of one hot-replica request (hot-window probe, /internal/v1 calls or pods) within a fan-out.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
		coldLists: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "cold_lists_total",
			Help: "S3 LIST requests issued by cold discovery.",
		}),
		partialResponses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "partial_responses_total",
			Help: "Responses served with partial=true (a source failed or degraded, 02 §7.4).",
		}),
		guardRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "guard_rejections_total",
			Help: "Wide-query guard rejections by layer (02 §2.3.2): span fires before any I/O, cost after the discovery LIST.",
		}, []string{"layer"}),
		budgetLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "read_budget_limit_bytes",
			Help: "Configured process-wide read memory budget (PROFILER_READ_MEMORY_BUDGET, 02 §7.5).",
		}),
		budgetUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "read_budget_used_bytes",
			Help: "Bytes currently charged against the read memory budget (02 §7.5).",
		}),
		budgetDenials: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "read_budget_denials_total",
			Help: "Requests answered 503 by the read budget (02 §7.5): reason=exhausted is transient contention, reason=never_fits is a structural misfit (a charge larger than the whole budget).",
		}, []string{"endpoint", "reason"}),
		budgetOverruns: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "profiler", Subsystem: "query",
			Name: "read_budget_batch_overruns_total",
			Help: "Post-read reconciles that found the actual batch or page footprint above its pre-read charge — the honesty signal of the §7.5 estimate.",
		}),
	}
	reg.MustRegister(m.fanoutSeconds, m.coldLists, m.partialResponses, m.guardRejections,
		m.budgetLimit, m.budgetUsed, m.budgetDenials, m.budgetOverruns)
	// Materialize the label values so the series exist from the first scrape.
	for _, result := range []string{"ok", "error"} {
		m.fanoutSeconds.WithLabelValues(result)
	}
	for _, layer := range []string{guardLayerSpan, guardLayerCost} {
		m.guardRejections.WithLabelValues(layer)
	}
	for _, endpoint := range []string{"calls", "point"} {
		for _, reason := range []string{"exhausted", "never_fits"} {
			m.budgetDenials.WithLabelValues(endpoint, reason)
		}
	}
	return m
}

// setBudgetLimit records the configured budget size at startup.
func (m *Metrics) setBudgetLimit(limit int64) {
	if m != nil {
		m.budgetLimit.Set(float64(limit))
	}
}

// observeBudgetUsed is the budget.Hooks.OnUsed adapter.
func (m *Metrics) observeBudgetUsed(used int64) {
	if m != nil {
		m.budgetUsed.Set(float64(used))
	}
}

// countBudgetDenial records one 503 answered for a budget denial.
func (m *Metrics) countBudgetDenial(endpoint, reason string) {
	if m != nil {
		m.budgetDenials.WithLabelValues(endpoint, reason).Inc()
	}
}

// countBudgetOverrun is the cold Source's OverrunHook adapter.
func (m *Metrics) countBudgetOverrun() {
	if m != nil {
		m.budgetOverruns.Inc()
	}
}

// observeFanout records one hot-replica round-trip.
func (m *Metrics) observeFanout(start time.Time, err error) {
	if m == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.fanoutSeconds.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

func (m *Metrics) countColdList() {
	if m != nil {
		m.coldLists.Inc()
	}
}

func (m *Metrics) countPartial(partial bool) {
	if m != nil && partial {
		m.partialResponses.Inc()
	}
}

func (m *Metrics) countGuardRejection(layer string) {
	if m != nil {
		m.guardRejections.WithLabelValues(layer).Inc()
	}
}

// countingColdStore decorates the cold ObjectStore to count LIST requests;
// Open and Get pass through untouched.
type countingColdStore struct {
	inner   cold.ObjectStore
	metrics *Metrics
}

func (c countingColdStore) List(ctx context.Context, prefix string) ([]cold.ObjectInfo, error) {
	c.metrics.countColdList()
	return c.inner.List(ctx, prefix)
}

func (c countingColdStore) Open(ctx context.Context, key string) (cold.Object, error) {
	return c.inner.Open(ctx, key)
}

func (c countingColdStore) Get(ctx context.Context, key string) ([]byte, error) {
	return c.inner.Get(ctx, key)
}
