package metrics

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/prometheus/client_golang/prometheus"
)

// truncatedReasons materializes every truncated_reason series up front so
// dashboards see a stable zero instead of a series that appears mid-incident.
var truncatedReasons = []string{
	hotstore.TruncDictMiss,
	hotstore.TruncDiskBudget,
	hotstore.TruncIdleTimeout,
	hotstore.TruncMemPressure,
}

// RegisterCollect wires the collect-process series over the store and
// uploader snapshots. Everything reads a cheap snapshot at scrape time — no
// collector holds a store lock across I/O. uploader may be nil (no object
// store composed); its series are simply absent then.
func RegisterCollect(reg prometheus.Registerer, store *hotstore.Store, uploader *hotstore.Uploader) {
	counter := func(subsystem, name, help string, value func() int64, labels prometheus.Labels) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: name, Help: help, ConstLabels: labels,
		}, func() float64 { return float64(value()) }))
	}
	gauge := func(subsystem, name, help string, value func() float64, labels prometheus.Labels) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem, Name: name, Help: help, ConstLabels: labels,
		}, value))
	}

	counter("seal", "rows_total", "Calls sealed into parquet rows (01 §6.5).",
		func() int64 { return store.SealCountersSnapshot().Rows }, nil)
	counter("seal", "files_total", "Parquet files produced by seal passes.",
		func() int64 { return store.SealCountersSnapshot().Files }, nil)
	for _, reason := range truncatedReasons {
		reason := reason
		counter("seal", "truncated_rows_total",
			"Rows sealed with trace_blob = NULL, by truncated_reason (01 §5.2).",
			func() int64 { return store.SealCountersSnapshot().Truncated[reason] },
			prometheus.Labels{"reason": reason})
	}

	if uploader != nil {
		upload := func(name, help string, value func(hotstore.UploadStats) int64) {
			counter("upload", name, help,
				func() int64 { return value(uploader.CountersSnapshot()) }, nil)
		}
		upload("uploaded_files_total", "Parquet files confirmed in S3 (01 §6.2).",
			func(s hotstore.UploadStats) int64 { return s.UploadedFiles })
		upload("put_failures_total", "Failed S3 PUT attempts, transient or permanent — the upload-failure alert reads this rate.",
			func(s hotstore.UploadStats) int64 { return s.FailedPuts })
		upload("retried_puts_total", "PUT attempts a retry followed (in-pass backoff, 01 §6.2).",
			func(s hotstore.UploadStats) int64 { return s.RetriedPuts })
		upload("quarantined_files_total", "Parquet files moved to upload-failed/ on a permanent rejection (01 §8).",
			func(s hotstore.UploadStats) int64 { return s.QuarantinedFiles })
		upload("quarantined_objects_total", "Snapshot/manifest bodies parked under upload-failed/ (01 §8).",
			func(s hotstore.UploadStats) int64 { return s.QuarantinedObjects })
		upload("snapshot_uploads_total", "Dictionary + suspend snapshot pairs uploaded (01 §3.6).",
			func(s hotstore.UploadStats) int64 { return s.SnapshotUploads })
		upload("manifest_puts_total", "pods/v1 manifest upserts (01 §3.6).",
			func(s hotstore.UploadStats) int64 { return s.ManifestPuts })
		upload("swept_segments_total", "Refcount-0 segments unlinked by the post-upload sweep (03 §3.7 step 14).",
			func(s hotstore.UploadStats) int64 { return s.SegmentsDeleted })
	}

	janitor := func(name, help string, value func(hotstore.JanitorStats) int64) {
		counter("janitor", name, help,
			func() int64 { return value(store.JanitorCountersSnapshot()) }, nil)
	}
	janitor("parquet_deleted_total", "Aged local parquet files deleted past hot retention (01 §6.3).",
		func(s hotstore.JanitorStats) int64 { return s.ParquetDeleted })
	janitor("partitions_dropped_total", "Call-index partitions dropped from the hot tier (02 §4.2).",
		func(s hotstore.JanitorStats) int64 { return s.PartitionsDropped })
	janitor("wals_purged_total", "Pod-restarts whose WAL files were purged (03 §3.9 step 18).",
		func(s hotstore.JanitorStats) int64 { return s.WalsPurged })
	janitor("segments_evicted_total", "Segments evicted under the disk budget (01 §4.6).",
		func(s hotstore.JanitorStats) int64 { return s.SegmentsEvicted })
	janitor("evicted_bytes_total", "Bytes freed by disk-budget evictions.",
		func(s hotstore.JanitorStats) int64 { return s.EvictedBytes })

	gauge("hotstore", "segments_disk_bytes",
		"On-disk bytes of the hot-store segment files, as of the last janitor pass.",
		func() float64 { bytes, _ := store.SegmentsDiskUsage(); return float64(bytes) }, nil)
	gauge("hotstore", "segments_disk_budget_bytes",
		"Configured segment disk budget (PROFILER_CHUNKS_STAGING_MAX_BYTES).",
		func() float64 { _, budget := store.SegmentsDiskUsage(); return float64(budget) }, nil)
	gauge("hotstore", "evicted_segment_chunk_refs",
		"In-RAM chunk-index entries pointing at evicted segments (risk B-3), as of the last janitor pass.",
		func() float64 { return float64(store.EvictedChunkRefs()) }, nil)
	gauge("hotstore", "hot_window_lag_seconds",
		"Age of the oldest row still in the hot index (now - hot_window_oldest_ms); 0 with an empty hot window. Sustained growth past hot retention means the hot→cold handoff is stuck.",
		func() float64 {
			oldest, ok, err := store.HotWindowOldestMs()
			if err != nil || !ok {
				return 0
			}
			return time.Since(time.UnixMilli(oldest)).Seconds()
		}, nil)

	reg.MustRegister(&quarantineCollector{store: store})
}

// quarantineCollector emits the four stuck-quarantine gauges from ONE
// QuarantineStats read per scrape (two SQLite queries), instead of one read
// per series.
type quarantineCollector struct {
	store *hotstore.Store
}

var (
	quarantineObjectsDesc = prometheus.NewDesc(
		namespace+"_hotstore_quarantine_objects",
		"Objects stuck in quarantine awaiting a human: parquet files under upload-failed/, or pod-restarts whose dictionary/suspend snapshot was rejected (01 §8). Shrinks only on manual intervention.",
		[]string{"kind"}, nil)
	quarantineAgeDesc = prometheus.NewDesc(
		namespace+"_hotstore_quarantine_oldest_age_seconds",
		"Age of the oldest quarantined object of each kind; 0 when the kind is empty.",
		[]string{"kind"}, nil)
)

func (c *quarantineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- quarantineObjectsDesc
	ch <- quarantineAgeDesc
}

func (c *quarantineCollector) Collect(ch chan<- prometheus.Metric) {
	stats, err := c.store.QuarantineStats()
	if err != nil {
		// Emit nothing rather than a fake zero: an absent series marks a broken
		// read, a zero would silently clear the stuck-quarantine alert.
		log.Error(context.Background(), err, "metrics: cannot read quarantine stats")
		return
	}
	age := func(oldestMs *int64) float64 {
		if oldestMs == nil {
			return 0
		}
		return time.Since(time.UnixMilli(*oldestMs)).Seconds()
	}
	ch <- prometheus.MustNewConstMetric(quarantineObjectsDesc, prometheus.GaugeValue,
		float64(stats.ParquetCount), "parquet")
	ch <- prometheus.MustNewConstMetric(quarantineAgeDesc, prometheus.GaugeValue,
		age(stats.ParquetOldestMs), "parquet")
	ch <- prometheus.MustNewConstMetric(quarantineObjectsDesc, prometheus.GaugeValue,
		float64(stats.SnapshotCount), "snapshot")
	ch <- prometheus.MustNewConstMetric(quarantineAgeDesc, prometheus.GaugeValue,
		age(stats.SnapshotOldestMs), "snapshot")
}
