package metrics

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/maintain"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRegisterCollectSeries pins the metric-name contract: dashboards and the
// PrometheusRule alerts reference these names, so a rename must fail a test.
func TestRegisterCollectSeries(t *testing.T) {
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	reg := NewRegistry()
	RegisterCollect(reg, store, hotstore.NewUploader(store, nil))

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(families))
	for _, mf := range families {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"profiler_seal_rows_total",
		"profiler_seal_files_total",
		"profiler_seal_truncated_rows_total",
		"profiler_upload_uploaded_files_total",
		"profiler_upload_put_failures_total",
		"profiler_upload_quarantined_files_total",
		"profiler_janitor_partitions_dropped_total",
		"profiler_janitor_segments_evicted_total",
		"profiler_hotstore_segments_disk_bytes",
		"profiler_hotstore_segments_disk_budget_bytes",
		"profiler_hotstore_hot_window_lag_seconds",
		"profiler_hotstore_quarantine_objects",
		"profiler_hotstore_quarantine_oldest_age_seconds",
		"profiler_hotstore_evicted_segment_chunk_refs",
		// Focus V additions (finding №14): loop-error counters, queue/backlog
		// gauges, and the single-total quarantine gauge.
		"profiler_seal_loop_errors_total",
		"profiler_upload_loop_errors_total",
		"profiler_janitor_loop_errors_total",
		"profiler_store_pods_size",
		"profiler_seal_queue_depth",
		"profiler_upload_backlog",
		"profiler_upload_lag_seconds",
		"profiler_quarantine_size",
		// Focus B additions (№1, №2): the mem budget, the pending-upload
		// budget with its two gates, and the quarantine retest/cap counters.
		"profiler_hotstore_inram_bytes",
		"profiler_hotstore_mem_budget_bytes",
		"profiler_hotstore_pending_parquet_bytes",
		"profiler_hotstore_partitions_disk_bytes",
		"profiler_hotstore_pending_budget_bytes",
		"profiler_backpressure_seal_paused",
		"profiler_backpressure_ingest_paused",
		"profiler_upload_requeued_files_total",
		"profiler_upload_requeued_snapshots_total",
		"profiler_janitor_quarantine_dropped_total",
		"profiler_janitor_snapshots_abandoned_total",
		"profiler_janitor_dicts_unloaded_total",
		"profiler_janitor_chunk_indexes_released_total",
		// Focus C additions (№7, №8, §6.1 trigger 3): lost big values, skipped
		// poisoned buckets, and mem-pressure early seals.
		"profiler_seal_lost_big_values_total",
		"profiler_seal_skipped_buckets_total",
		"profiler_janitor_mem_pressure_seals_total",
	} {
		assert.True(t, names[want], "missing series %s", want)
	}
}

// TestRegisterS3Series pins that the shared cdt_minio_* series land on a
// subcommand's own registry (finding №14): before this, they only registered
// on the Prometheus default registry, so no subcommand's /metrics exposed them.
func TestRegisterS3Series(t *testing.T) {
	reg := NewRegistry()
	s3.RegisterMetrics(reg)
	// A second call must be a no-op, not a panic: several MinioClients share one
	// process registry.
	s3.RegisterMetrics(reg)

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(families))
	for _, mf := range families {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"cdt_minio_operation_latency_seconds",
		"cdt_minio_operation_objects_count",
		"cdt_minio_operation_errors_count",
	} {
		assert.True(t, names[want], "missing series %s", want)
	}
}

// TestRegisterMaintainObserve pins that per-pass stats accumulate into the
// counters, including the labelled families.
func TestRegisterMaintainObserve(t *testing.T) {
	reg := NewRegistry()
	m := RegisterMaintain(reg)
	m.Observe(maintain.Stats{CompactedGroups: 2, TTLParquetDeleted: 3, SkippedUnsettled: 1})
	m.Observe(maintain.Stats{CompactedGroups: 1})

	families, err := reg.Gather()
	require.NoError(t, err)
	value := func(name string, labels map[string]string) float64 {
		for _, mf := range families {
			if mf.GetName() != name {
				continue
			}
		metric:
			for _, met := range mf.GetMetric() {
				for k, v := range labels {
					found := false
					for _, lp := range met.GetLabel() {
						if lp.GetName() == k && lp.GetValue() == v {
							found = true
						}
					}
					if !found {
						continue metric
					}
				}
				return met.GetCounter().GetValue()
			}
		}
		return -1
	}
	assert.EqualValues(t, 3, value("profiler_maintain_compacted_groups_total", nil))
	assert.EqualValues(t, 2, value("profiler_maintain_passes_total", nil))
	assert.EqualValues(t, 3, value("profiler_maintain_ttl_deleted_objects_total", map[string]string{"kind": "parquet"}))
	assert.EqualValues(t, 1, value("profiler_maintain_skipped_groups_total", map[string]string{"reason": "unsettled"}))
}
