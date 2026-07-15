package metrics

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/maintain"
	"github.com/prometheus/client_golang/prometheus"
)

// MaintainMetrics accumulates the maintain-job series from per-pass Stats.
// Unlike the collect side there is no lifetime snapshot to read, so these are
// real counters fed by the Job.OnPass observer.
type MaintainMetrics struct {
	passes              prometheus.Counter
	passErrors          prometheus.Counter
	compactedGroups     prometheus.Counter
	compactedInputFiles prometheus.Counter
	compactedRows       prometheus.Counter
	dedupedRows         prometheus.Counter
	deletedInputFiles   prometheus.Counter
	skippedGroups       *prometheus.CounterVec
	ttlDeletedObjects   *prometheus.CounterVec
}

// RegisterMaintain registers the maintain series on reg.
func RegisterMaintain(reg prometheus.Registerer) *MaintainMetrics {
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "maintain", Name: name, Help: help,
		})
		reg.MustRegister(c)
		return c
	}
	m := &MaintainMetrics{
		passes:              counter("passes_total", "Completed maintenance passes."),
		passErrors:          counter("pass_errors_total", "Failures a pass logged and skipped over (one bad object never stalls the rest)."),
		compactedGroups:     counter("compacted_groups_total", "Fresh compactions: one merged object written each (01 §6.6)."),
		compactedInputFiles: counter("compacted_input_files_total", "Input objects consumed by fresh compactions."),
		compactedRows:       counter("compacted_rows_total", "Rows written into compacted objects."),
		dedupedRows:         counter("deduped_rows_total", "Duplicate-PK rows dropped by the compaction merge."),
		deletedInputFiles:   counter("deleted_input_files_total", "Compaction inputs deleted after the delete grace."),
		skippedGroups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "maintain", Name: "skipped_groups_total",
			Help: "Compaction groups skipped, by reason: small (below MIN_FILES), unsettled (younger than MIN_AGE), oversized (over MAX_BYTES).",
		}, []string{"reason"}),
		ttlDeletedObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "maintain", Name: "ttl_deleted_objects_total",
			Help: "Objects expired by the retention TTLs, by kind (01 §6.4, §3.6).",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.skippedGroups, m.ttlDeletedObjects)
	for _, reason := range []string{"small", "unsettled", "oversized"} {
		m.skippedGroups.WithLabelValues(reason)
	}
	for _, kind := range []string{"parquet", "pods_manifest"} {
		m.ttlDeletedObjects.WithLabelValues(kind)
	}
	return m
}

// Observe adds one pass's stats; wire it as Job.OnPass.
func (m *MaintainMetrics) Observe(stats maintain.Stats) {
	m.passes.Inc()
	m.passErrors.Add(float64(stats.Errors))
	m.compactedGroups.Add(float64(stats.CompactedGroups))
	m.compactedInputFiles.Add(float64(stats.CompactedInputFiles))
	m.compactedRows.Add(float64(stats.CompactedRows))
	m.dedupedRows.Add(float64(stats.DedupedRows))
	m.deletedInputFiles.Add(float64(stats.DeletedInputFiles))
	m.skippedGroups.WithLabelValues("small").Add(float64(stats.SkippedSmallGroups))
	m.skippedGroups.WithLabelValues("unsettled").Add(float64(stats.SkippedUnsettled))
	m.skippedGroups.WithLabelValues("oversized").Add(float64(stats.SkippedOversized))
	m.ttlDeletedObjects.WithLabelValues("parquet").Add(float64(stats.TTLParquetDeleted))
	m.ttlDeletedObjects.WithLabelValues("pods_manifest").Add(float64(stats.TTLManifestsDeleted))
}
