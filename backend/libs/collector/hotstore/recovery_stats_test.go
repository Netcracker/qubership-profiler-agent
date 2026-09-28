package hotstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryStatsProgress(t *testing.T) {
	var s RecoveryStats
	s.PodRestartsFound.Store(5)
	s.PodRestartsProcessed.Store(2)
	assert.Contains(t, s.Progress(), "2 of 5 pod-restarts")
}

// TestCountRecoveryFailed pins that only directories count: a quarantined
// pod-restart is a directory, and a stray file beside them is not one.
func TestCountRecoveryFailed(t *testing.T) {
	t.Run("missing directory counts zero", func(t *testing.T) {
		n, err := CountRecoveryFailed(t.TempDir())
		require.NoError(t, err)
		assert.Zero(t, n)
	})
	t.Run("counts subdirectories, not files", func(t *testing.T) {
		dataDir := t.TempDir()
		root := filepath.Join(dataDir, "recovery-failed")
		require.NoError(t, os.MkdirAll(filepath.Join(root, "ns_svc_pod-a_1000"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(root, "ns_svc_pod-b_1000"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644))
		n, err := CountRecoveryFailed(dataDir)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})
	t.Run("a regular file at the path is an error", func(t *testing.T) {
		dataDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, "recovery-failed"), []byte("x"), 0o644))
		_, err := CountRecoveryFailed(dataDir)
		assert.Error(t, err)
	})
}
