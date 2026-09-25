package metrics

import (
	"context"
	"sync/atomic"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterRecovery wires the startup-recovery series over stats and the
// recovery-failed/ directory under dataDir. Register it before the store
// opens: stats fills in while Recover runs, and /metrics serves during
// recovery. The counters cover this process only; the persistent signal is
// recovery_failed_pod_restarts.
func RegisterRecovery(reg prometheus.Registerer, stats *hotstore.RecoveryStats, dataDir string) {
	value := func(v *atomic.Int64) func() float64 {
		return func() float64 { return float64(v.Load()) }
	}
	gauge := func(name, help string, v *atomic.Int64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "recovery", Name: name, Help: help,
		}, value(v)))
	}
	counter := func(name, help string, v *atomic.Int64, labels prometheus.Labels) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "recovery", Name: name, Help: help, ConstLabels: labels,
		}, value(v)))
	}

	gauge("pod_restarts_found",
		"Pod-restart directories startup recovery found on the PV (03 §3).",
		&stats.PodRestartsFound)
	gauge("pod_restarts_processed",
		"Pod-restarts startup recovery has recovered or quarantined so far; equals pod_restarts_found once it is done.",
		&stats.PodRestartsProcessed)
	counter("quarantined_pod_restarts_total",
		"Pod-restarts startup recovery moved under recovery-failed/ because their on-disk state did not recover (№26).",
		&stats.QuarantinedPodRestarts, nil)
	counter("orphan_parquet_removed_total",
		"Parquet files with no catalog row that startup recovery deleted; their rows re-seal from the watermark.",
		&stats.OrphanParquetRemoved, nil)
	counter("lost_pending_parquet_total",
		"Pending parquet files missing on disk before upload; recovery rewound their seal watermark so the calls re-seal (03 §3.6).",
		&stats.LostPendingParquet, nil)
	droppedHelp := "Call-index rows startup recovery dropped, by cause: torn_wal_tail rows point past the end of a torn calls.wal (№8), quarantine rows belong to a quarantined pod-restart. Their calls are lost to the hot tier."
	counter("dropped_index_rows_total", droppedHelp,
		&stats.DroppedIndexRowsTornTail, prometheus.Labels{"cause": "torn_wal_tail"})
	counter("dropped_index_rows_total", droppedHelp,
		&stats.DroppedIndexRowsQuarantine, prometheus.Labels{"cause": "quarantine"})

	reg.MustRegister(&recoveryFailedCollector{dataDir: dataDir})
}

// recoveryFailedCollector counts the recovery-failed/ directories at scrape
// time, so the gauge survives restarts until a human clears them.
type recoveryFailedCollector struct {
	dataDir string
}

var recoveryFailedDesc = prometheus.NewDesc(
	namespace+"_recovery_failed_pod_restarts",
	"Pod-restart directories under recovery-failed/ waiting for a human (№26); stays non-zero across restarts until they are removed.",
	nil, nil)

func (c *recoveryFailedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- recoveryFailedDesc
}

func (c *recoveryFailedCollector) Collect(ch chan<- prometheus.Metric) {
	n, err := hotstore.CountRecoveryFailed(c.dataDir)
	if err != nil {
		// Emit nothing rather than a fake zero: an absent series marks a broken
		// read, a zero would read as nothing waiting for a human.
		log.Error(context.Background(), err, "metrics: cannot count recovery-failed pod-restarts")
		return
	}
	ch <- prometheus.MustNewConstMetric(recoveryFailedDesc, prometheus.GaugeValue, float64(n))
}
