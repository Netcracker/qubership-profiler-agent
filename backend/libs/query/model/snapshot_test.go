package model_test

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
)

// TestPodRestartHashMatchesWriter pins the reader-side hash to the one the
// seal pass stamps into object keys: a divergence would strand every pods
// manifest and every point fetch.
func TestPodRestartHashMatchesWriter(t *testing.T) {
	key := hotstore.PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 42}
	assert.Equal(t, hotstore.PodRestartHash(key), model.PodRestartHash(key.Tuple()))
}

// TestPodRestartHashWidth pins the №22 widening: the hash keys parquet files
// and the pods manifests, and the old 4 bytes reached 50% collision odds
// around 77k pod-restarts — weeks of a 500-pod cluster's churn inside one
// retention window. 12 bytes (24 hex chars) puts the birthday bound beyond
// any realistic object count.
func TestPodRestartHashWidth(t *testing.T) {
	tuple := model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: 42}
	assert.Len(t, model.PodRestartHash(tuple), 24)
}
