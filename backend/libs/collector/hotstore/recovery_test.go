package hotstore

// №26 recovery robustness: one broken pod-restart quarantines instead of
// crash-looping the collector, and a crash between the seal rename and the
// pass commit leaves no orphan sealed files behind. The crashes are genuine:
// the store closes without footering the WALs and the corruption is injected
// into the on-disk bytes.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecoverQuarantinesCorruptPodRestart pins the 03 §4 "degrade, not fail"
// behaviour: a pod-restart whose dictionary.wal replays into garbage — a
// structurally valid record with an undecodable body, the kind of damage a
// torn page leaves behind — is quarantined under recovery-failed/ while every
// other pod-restart recovers and the collector starts.
func TestRecoverQuarantinesCorruptPodRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)

	open := func(pod string) *PodRestart {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1_000})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Api.get")
		require.NoError(t, err)
		require.NoError(t, pr.AppendCall(1_700_000_000_000, data.Call{
			Method: 0, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		}))
		return pr
	}
	corrupt, healthy := open("pod-corrupt"), open("pod-ok")
	corruptDir, healthyKey := corrupt.dir, healthy.Key
	bucket := store.cfg.Bucket(1_700_000_000_000)

	// kill -9, then the damage: a record whose framing and CRC are fine but
	// whose body is not a dictionary entry, so the replay hard-fails.
	require.NoError(t, store.Close())
	w, err := OpenWal(filepath.Join(corruptDir, "dictionary.wal"), 1, time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte{0xFF}) // a lone continuation byte: no valid varint
	require.NoError(t, err)
	require.NoError(t, w.Sync())

	store, err = Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	require.NoError(t, store.Recover(ctx), "one broken pod-restart must not fail recovery")

	_, ok := store.PodRestart(PodRestartKey{
		Namespace: "ns", Service: "svc", PodName: "pod-corrupt", RestartTimeMs: 1_000})
	assert.False(t, ok, "the broken pod-restart is not resurrected")
	assert.NoDirExists(t, corruptDir, "its directory left the pods/ tree")
	assert.DirExists(t, filepath.Join(dataDir, "recovery-failed", "ns_svc_pod-corrupt_1000"),
		"the directory waits under recovery-failed/ for a human")

	rows, err := store.Calls(bucket)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the quarantined pod-restart's index rows are purged")
	assert.Equal(t, healthyKey.String(), rows[0].PodRestart)

	_, ok = store.PodRestart(healthyKey)
	assert.True(t, ok, "the healthy pod-restart recovered")
	sealed, err := store.SealDue(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Equal(t, 1, sealed, "the healthy bucket seals; nothing waits on the quarantined one")
}

// TestRecoverRemovesOrphanSealedParquet pins the №6 crash-window cleanup: a
// sealed file with no parquet_local row (kill -9 between the rename and the
// pass commit) is swept on recovery, while catalogued files stay.
func TestRecoverRemovesOrphanSealedParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-o", RestartTimeMs: 1_000}
	require.NoError(t, store.db.UpsertPodRestart(key, 1_000))

	sealedDir := filepath.Join(dataDir, "parquet", "v1", "short_clean", "2023", "11", "14", "22")
	require.NoError(t, os.MkdirAll(sealedDir, 0o755))
	catalogued := filepath.Join(sealedDir, "collector-0-aaaa-x-x-x-0.parquet")
	orphan := filepath.Join(sealedDir, "collector-0-bbbb-x-x-x-0.parquet")
	for _, p := range []string{catalogued, orphan} {
		require.NoError(t, os.WriteFile(p, []byte("parquet"), 0o644))
	}
	require.NoError(t, store.db.RecordSealedFile(parquetLocalRow{
		Path: catalogued, PodRestart: key.String(), TimeBucketMs: 0,
		RetentionClass: RetentionShortClean, Seq: 0, RowCount: 1,
		TimeMinMs: 1, TimeMaxMs: 2, FileSize: 7, SealedAtMs: 1,
		S3Key: "parquet/v1/short_clean/x/a.parquet",
	}, nil))

	require.NoError(t, store.Recover(ctx))
	assert.FileExists(t, catalogued, "a catalogued file survives recovery")
	assert.NoFileExists(t, orphan, "an uncommitted seal's file is swept")
}
