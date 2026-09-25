package metrics

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherValues maps every series of reg to its value, keyed by the family
// name followed by its labels in braces when it has any.
func gatherValues(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			if len(m.GetLabel()) > 0 {
				pairs := make([]string, 0, len(m.GetLabel()))
				for _, lp := range m.GetLabel() {
					pairs = append(pairs, lp.GetName()+"="+lp.GetValue())
				}
				sort.Strings(pairs)
				key += "{" + strings.Join(pairs, ",") + "}"
			}
			value := m.GetGauge().GetValue()
			if m.GetCounter() != nil {
				value = m.GetCounter().GetValue()
			}
			out[key] = value
		}
	}
	return out
}

// TestRegisterRecoverySeries pins the recovery series names and that each
// reads its own RecoveryStats field: every field holds a distinct value.
func TestRegisterRecoverySeries(t *testing.T) {
	dataDir := t.TempDir()
	for _, dir := range []string{"ns_svc_pod-a_1000", "ns_svc_pod-b_1000", "ns_svc_pod-c_1000"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "recovery-failed", dir), 0o755))
	}
	stats := &hotstore.RecoveryStats{}
	stats.PodRestartsFound.Store(10)
	stats.PodRestartsProcessed.Store(9)
	stats.QuarantinedPodRestarts.Store(2)
	stats.OrphanParquetRemoved.Store(4)
	stats.LostPendingParquet.Store(5)
	stats.DroppedIndexRowsTornTail.Store(6)
	stats.DroppedIndexRowsQuarantine.Store(7)

	reg := NewRegistry()
	RegisterRecovery(reg, stats, dataDir)
	values := gatherValues(t, reg)
	for name, want := range map[string]float64{
		"profiler_recovery_pod_restarts_found":                            10,
		"profiler_recovery_pod_restarts_processed":                        9,
		"profiler_recovery_quarantined_pod_restarts_total":                2,
		"profiler_recovery_orphan_parquet_removed_total":                  4,
		"profiler_recovery_lost_pending_parquet_total":                    5,
		`profiler_recovery_dropped_index_rows_total{cause=torn_wal_tail}`: 6,
		`profiler_recovery_dropped_index_rows_total{cause=quarantine}`:    7,
		"profiler_recovery_failed_pod_restarts":                           3,
	} {
		got, ok := values[name]
		if assert.True(t, ok, "missing series %s", name) {
			assert.Equal(t, want, got, name)
		}
	}
}

// TestRecoveryFailedAbsentOnReadError pins that an unreadable recovery-failed
// path drops the gauge instead of reporting zero, which would read as nothing
// waiting for a human.
func TestRecoveryFailedAbsentOnReadError(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "recovery-failed"), []byte("x"), 0o644))

	reg := NewRegistry()
	RegisterRecovery(reg, &hotstore.RecoveryStats{}, dataDir)
	values := gatherValues(t, reg)
	assert.NotContains(t, values, "profiler_recovery_failed_pod_restarts")
	assert.Contains(t, values, "profiler_recovery_pod_restarts_found", "the other series still register")
}
