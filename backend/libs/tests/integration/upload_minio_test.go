//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUploadPassMinio proves the S3ObjectStore wiring against a real MinIO:
// the sealed parquet lands byte-identical at its recorded key, the snapshot
// objects appear, and a 4xx (missing bucket) classifies as permanent. The
// fault-injection scenarios live in TestUploadPass on the in-test fake.
func TestUploadPassMinio(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	mc := helpers.CreateMinioContainer(ctx)
	defer func() { _ = mc.Terminate(ctx) }()

	svc := startCollector(t, ctx, t.TempDir())
	store := svc.Store()

	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
	}

	ac := connectAgent(t, ctx)
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)

	res, err := store.Seal(ctx, key, store.Config().Bucket(baseMs+5))
	require.NoError(t, err)
	require.Len(t, res.Files, 1)
	sealed := res.Files[0]
	localData, err := os.ReadFile(sealed.Path)
	require.NoError(t, err)

	uploader := hotstore.NewUploader(store, collector.NewS3ObjectStore(mc.Client))
	stats, err := uploader.Pass(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.UploadedFiles)
	assert.EqualValues(t, 1, stats.SnapshotUploads)
	assert.EqualValues(t, 1, stats.ManifestPuts)

	assert.Equal(t, localData, getObjectBytes(t, ctx, mc.Client, sealed.S3Key),
		"the parquet must round-trip byte-identical through MinIO")

	hash := hotstore.PodRestartHash(key)
	day := time.UnixMilli(key.RestartTimeMs).UTC().Format("2006/01/02")
	for _, prefix := range []string{"dictionaries/v1/" + day + "/" + hash, "suspend/v1/" + day + "/" + hash} {
		objects, err := mc.Client.ListObjectsWithPrefix(ctx, prefix)
		require.NoError(t, err)
		assert.Len(t, objects, 1, "snapshot object at %s", prefix)
	}

	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.NotNil(t, files[0].UploadedAtMs)

	t.Run("a real 4xx classifies as permanent", func(t *testing.T) {
		broken := *mc.Client
		broken.Params.BucketName = "no-such-bucket"
		err := collector.NewS3ObjectStore(&broken).PutBytes(ctx, "x.json", []byte("{}"))
		require.Error(t, err)
		assert.True(t, hotstore.IsPermanentUploadError(err),
			"NoSuchBucket is a 404 the retry loop cannot fix")
	})
}

func getObjectBytes(t *testing.T, ctx context.Context, mc *s3.MinioClient, key string) []byte {
	obj, err := mc.Client.GetObject(ctx, mc.Bucket(), key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	require.NoError(t, err)
	return data
}
