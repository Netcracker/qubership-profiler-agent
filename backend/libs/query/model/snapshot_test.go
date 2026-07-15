package model_test

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
)

// TestDictionarySnapshotKeyDay pins the key's day component to the UTC day of
// restart_time_ms — not the seal, close, or query day (01 §3.6, decisions
// log 2026-07-03). A pod that restarts before midnight and keeps producing
// calls after it must resolve to the restart day.
func TestDictionarySnapshotKeyDay(t *testing.T) {
	tuple := model.PodTuple{
		Namespace: "ns", Service: "svc", Pod: "pod",
		RestartTimeMs: 1782863999500, // 2026-06-30T23:59:59.5Z
	}
	key := model.DictionarySnapshotKey(tuple)
	assert.Equal(t, "dictionaries/v1/2026/06/30/"+model.PodRestartHash(tuple)+".json", key)
}

// TestPodRestartHashMatchesWriter pins the reader-side hash to the one the
// seal pass stamps into object keys: a divergence would strand every cold
// dictionary and every point fetch.
func TestPodRestartHashMatchesWriter(t *testing.T) {
	key := hotstore.PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 42}
	assert.Equal(t, hotstore.PodRestartHash(key), model.PodRestartHash(key.Tuple()))
}
