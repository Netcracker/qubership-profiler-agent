package metrics

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/ingest"
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
	counter("seal", "loop_errors_total",
		"Failed seal passes (the loop logged and retried on the next tick). A sustained rate means sealing is wedged.",
		func() int64 { return store.SealLoopErrors() }, nil)
	counter("seal", "lost_big_values_total",
		"Big-parameter values a seal could not resolve because their value segment was evicted or torn (№7); each loss truncates its row with disk_budget.",
		func() int64 { return store.SealCountersSnapshot().LostBigValues }, nil)
	counter("seal", "skipped_buckets_total",
		"Pod-restart buckets a seal pass skipped after a failure (№8). Retried every pass; a sustained rate marks a poisoned bucket.",
		func() int64 { return store.SealSkippedBuckets() }, nil)
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
		upload("quarantined_objects_total", "Manifest bodies parked under upload-failed/ (01 §8).",
			func(s hotstore.UploadStats) int64 { return s.QuarantinedObjects })
		upload("manifest_puts_total", "pods/v1 manifest upserts (01 §3.6).",
			func(s hotstore.UploadStats) int64 { return s.ManifestPuts })
		upload("swept_segments_total", "Refcount-0 segments unlinked by the post-upload sweep (03 §3.7 step 14).",
			func(s hotstore.UploadStats) int64 { return s.SegmentsDeleted })
		upload("requeued_files_total", "Quarantined parquet files re-queued by the slow re-test (№2).",
			func(s hotstore.UploadStats) int64 { return s.RequeuedFiles })
		counter("upload", "loop_errors_total",
			"Failed upload passes (whole-pass failures the loop retried), distinct from per-file put_failures_total.",
			func() int64 { return uploader.LoopErrors() }, nil)
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
	janitor("quarantine_dropped_total",
		"Quarantined parquet files dropped by the age/size cap (№2) — bounded, logged data loss that unpins the WAL purge.",
		func(s hotstore.JanitorStats) int64 { return s.QuarantineDropped })
	janitor("dicts_unloaded_total",
		"Closed pod-restarts whose dictionary maps the mem budget unloaded (№1); they reload from the WAL on demand.",
		func(s hotstore.JanitorStats) int64 { return s.DictionariesUnloaded })
	janitor("chunk_indexes_released_total",
		"Fully-sealed closed pod-restarts whose chunk index the mem budget released (№1); hot trace reads fall through to cold.",
		func(s hotstore.JanitorStats) int64 { return s.ChunkIndexesReleased })
	janitor("mem_pressure_seals_total",
		"Pod-restart buckets early-sealed by the mem budget (01 §6.1 trigger 3) to unpin their chunk indexes.",
		func(s hotstore.JanitorStats) int64 { return s.MemPressureSeals })
	counter("janitor", "loop_errors_total",
		"Failed janitor passes (the loop logged and retried on the next tick). A sustained rate means retention/eviction is wedged.",
		func() int64 { return store.JanitorLoopErrors() }, nil)

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

	gauge("hotstore", "inram_bytes",
		"In-RAM pod-restart state (dictionaries, chunk indexes, pause mirrors), as of the last janitor mem-budget step (№1).",
		func() float64 { bytes, _ := store.MemUsage(); return float64(bytes) }, nil)
	gauge("hotstore", "mem_budget_bytes",
		"Configured in-RAM budget (PROFILER_MEM_BUDGET).",
		func() float64 { _, budget := store.MemUsage(); return float64(budget) }, nil)
	gauge("hotstore", "pending_parquet_bytes",
		"Sealed parquet bytes not confirmed in S3 (pending + quarantined), as of the last backpressure refresh (№2).",
		func() float64 { parquet, _, _ := store.PendingUploadUsage(); return float64(parquet) }, nil)
	gauge("hotstore", "partitions_disk_bytes",
		"On-disk bytes of the live call-index partitions, as of the last backpressure refresh (№2).",
		func() float64 { _, partitions, _ := store.PendingUploadUsage(); return float64(partitions) }, nil)
	gauge("hotstore", "pending_budget_bytes",
		"Configured pending-upload budget (PROFILER_PENDING_UPLOAD_MAX_BYTES): sealing pauses once pending parquet reaches half of it, ingest once the whole backlog reaches it.",
		func() float64 { _, _, budget := store.PendingUploadUsage(); return float64(budget) }, nil)
	gauge("backpressure", "seal_paused",
		"1 while the seal loop is paused by the pending-upload budget (№2).",
		func() float64 { return boolGauge(store.SealPaused()) }, nil)
	gauge("backpressure", "ingest_paused",
		"1 while ingest refuses agent data under the pending-upload budget (№2); the agent buffers and retries.",
		func() float64 { return boolGauge(store.IngestPaused()) }, nil)

	gauge("store", "pods_size",
		"Pod-restarts the hot store tracks in RAM. The mem budget (№1) bounds each entry's footprint; the entry itself lives until the WAL purge, so sustained growth past the purge horizon still signals a leak.",
		func() float64 { return float64(store.PodsSize()) }, nil)

	gauge("seal", "queue_depth",
		"Pod-restart buckets waiting to be sealed, as of the last SealDue pass. Grows while backpressure pauses sealing (№2).",
		func() float64 { return float64(store.SealQueueDepth()) }, nil)

	reg.MustRegister(&quarantineCollector{store: store})
	reg.MustRegister(&backlogCollector{store: store})
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// RegisterIngest wires the agent-ingest counters (№21) onto the same registry.
// Every value reads an atomic snapshot at scrape time — no lock is held. These
// replace the no-op metric stubs the ingest listener carried before.
func RegisterIngest(reg prometheus.Registerer, listener *ingest.Listener) {
	if listener == nil {
		return
	}
	counter := func(name, help string, value func(ingest.IngestStats) int64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: name, Help: help,
		}, func() float64 { return float64(value(listener.IngestStatsSnapshot())) }))
	}
	counter("commands_total", "Agent commands dispatched to the ingest listener.",
		func(s ingest.IngestStats) int64 { return int64(s.CommandsReceived) })
	counter("command_errors_total", "Agent commands the listener failed (unknown command, decode error, pre-handshake data).",
		func(s ingest.IngestStats) int64 { return int64(s.CommandErrors) })
	counter("bytes_total", "RCV_DATA payload bytes routed into stream files.",
		func(s ingest.IngestStats) int64 { return int64(s.BytesRead) })
	counter("decoder_errors_total", "Streams a decoder rejected as malformed or gzip-wrapped; the agent resends from scratch (06 §6).",
		func(s ingest.IngestStats) int64 { return int64(s.DecoderErrors) })
}

// backlogCollector emits the upload-backlog gauges from ONE UploadBacklog read
// per scrape (a single COUNT/MIN query), instead of one query per series.
type backlogCollector struct {
	store *hotstore.Store
}

var (
	uploadBacklogDesc = prometheus.NewDesc(
		namespace+"_upload_backlog",
		"Sealed parquet files still owed to S3 (uploaded_at IS NULL, not quarantined). Sustained growth means the upload loop cannot keep up.",
		nil, nil)
	uploadLagDesc = prometheus.NewDesc(
		namespace+"_upload_lag_seconds",
		"Age of the oldest pending upload (now - sealed_at); 0 when the queue is empty.",
		nil, nil)
)

func (c *backlogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- uploadBacklogDesc
	ch <- uploadLagDesc
}

func (c *backlogCollector) Collect(ch chan<- prometheus.Metric) {
	count, oldestSealedMs, err := c.store.UploadBacklog()
	if err != nil {
		// Emit nothing rather than a fake zero: an absent series marks a broken
		// read, a zero would silently clear a backlog alert.
		log.Error(context.Background(), err, "metrics: cannot read upload backlog")
		return
	}
	ch <- prometheus.MustNewConstMetric(uploadBacklogDesc, prometheus.GaugeValue, float64(count))
	lag := 0.0
	if oldestSealedMs != nil {
		lag = time.Since(time.UnixMilli(*oldestSealedMs)).Seconds()
	}
	ch <- prometheus.MustNewConstMetric(uploadLagDesc, prometheus.GaugeValue, lag)
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
		"Objects stuck in quarantine awaiting a human: parquet files under upload-failed/ (01 §8). Shrinks only on manual intervention.",
		[]string{"kind"}, nil)
	quarantineAgeDesc = prometheus.NewDesc(
		namespace+"_hotstore_quarantine_oldest_age_seconds",
		"Age of the oldest quarantined object of each kind; 0 when the kind is empty.",
		[]string{"kind"}, nil)
	quarantineSizeDesc = prometheus.NewDesc(
		namespace+"_quarantine_size",
		"Total objects stuck in quarantine. A single non-zero alerting signal over the per-kind breakdown.",
		nil, nil)
)

func (c *quarantineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- quarantineObjectsDesc
	ch <- quarantineAgeDesc
	ch <- quarantineSizeDesc
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
	ch <- prometheus.MustNewConstMetric(quarantineSizeDesc, prometheus.GaugeValue,
		float64(stats.ParquetCount))
}
