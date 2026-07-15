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
	}
	reg.MustRegister(m.fanoutSeconds, m.coldLists, m.partialResponses, m.guardRejections)
	// Materialize the label values so the series exist from the first scrape.
	for _, result := range []string{"ok", "error"} {
		m.fanoutSeconds.WithLabelValues(result)
	}
	for _, layer := range []string{guardLayerSpan, guardLayerCost} {
		m.guardRejections.WithLabelValues(layer)
	}
	return m
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
