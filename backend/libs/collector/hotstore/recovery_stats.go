package hotstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
)

// RecoveryStats counts what Recover did to the PV, one field per kind of
// action. Every field is written where the action happens, so a scrape during
// recovery reads the progress so far. The zero value is ready to use; share
// it by pointer through Config.Recovery so the metrics can be registered
// before the store opens.
type RecoveryStats struct {
	// PodRestartsFound is the number of pod-restart directories Recover found
	// under pods/, and PodRestartsProcessed how many of them it has recovered
	// or quarantined so far.
	PodRestartsFound     atomic.Int64
	PodRestartsProcessed atomic.Int64
	// QuarantinedPodRestarts counts pod-restarts moved under recovery-failed/
	// (№26).
	QuarantinedPodRestarts atomic.Int64
	// OrphanParquetRemoved counts parquet files under parquet/ or
	// upload-failed/ with no parquet_local row that recovery deleted.
	OrphanParquetRemoved atomic.Int64
	// LostPendingParquet counts pending parquet files missing on disk whose
	// bucket seal watermark recovery rewound so the calls re-seal (03 §3.6).
	LostPendingParquet atomic.Int64
	// DroppedIndexRowsTornTail counts index rows dropped because they point
	// past the end of a torn calls.wal (№8); DroppedIndexRowsQuarantine
	// counts the index rows of quarantined pod-restarts.
	DroppedIndexRowsTornTail   atomic.Int64
	DroppedIndexRowsQuarantine atomic.Int64
}

// Progress describes how far Recover has got, for the readiness details.
func (s *RecoveryStats) Progress() string {
	return fmt.Sprintf("recovering the hot store: %d of %d pod-restarts",
		s.PodRestartsProcessed.Load(), s.PodRestartsFound.Load())
}

// CountRecoveryFailed returns the number of pod-restart directories waiting
// for a human under <dataDir>/recovery-failed. A missing directory counts 0.
func CountRecoveryFailed(dataDir string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(dataDir, "recovery-failed"))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrap(err, "read recovery-failed dir")
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			n++
		}
	}
	return n, nil
}
