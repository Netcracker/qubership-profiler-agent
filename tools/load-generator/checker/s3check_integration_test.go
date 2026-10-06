//go:build integration

package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
)

// TestMinioListerAgainstContainer exercises the §8.5 production lister
// against a real MinIO (testcontainers), the same harness the libs/tests
// integration suite uses: seal-style keys land under a path prefix, the
// lister strips it, and foreign objects are skipped.
func TestMinioListerAgainstContainer(t *testing.T) {
	ctx := context.Background()
	mc := helpers.CreateMinioContainer(ctx)
	t.Cleanup(func() { _ = mc.Terminate(ctx) })

	const pathPrefix = "stand-a"
	bucketStart := time.Date(2026, 7, 16, 10, 5, 0, 0, time.UTC)
	put := func(key string, size int) {
		_, err := mc.Client.Client.PutObject(ctx, mc.Params.BucketName, key,
			bytes.NewReader(make([]byte, size)), int64(size), minio.PutObjectOptions{})
		require.NoError(t, err)
	}
	put(pathPrefix+"/"+sealKey("normal_clean", bucketStart, "collector-0", 0), 512)
	put(pathPrefix+"/"+sealKey("normal_clean", bucketStart, "collector-0", 1), 2<<20)
	put(pathPrefix+"/"+sealKey("short_clean", bucketStart, maintainReplica, 0), 4<<20)
	put(pathPrefix+"/parquet/v1/normal_clean/2026/07/16/10/foreign.txt", 16) // skipped by the parser
	put("other-stand/parquet/v1/normal_clean/2026/07/16/10/x.parquet", 16)   // outside the prefix

	lister, err := newMinioLister(ctx, *mc.Params, pathPrefix)
	require.NoError(t, err)
	objects, err := lister.List(ctx)
	require.NoError(t, err)
	require.Len(t, objects, 4, "everything under the prefix, nothing outside it")

	sample := newS3Sample(time.Now(), objects, fastTimers())
	normal := hourKey{class: "normal_clean", hour: "2026/07/16/10"}
	assert.Equal(t, 2, sample.hours[normal].objects, "the foreign key is skipped")
	assert.Equal(t, 1, sample.hours[normal].small, "one of the two parquet objects is under 1 MB")
	assert.Equal(t, 2, sample.sealed[groupKey{class: "normal_clean", bucketStartMs: bucketStart.UnixMilli()}])
	assert.Zero(t, sample.sealed[groupKey{class: "short_clean", bucketStartMs: bucketStart.UnixMilli()}],
		"compacted outputs never count against the compaction trigger")
}
