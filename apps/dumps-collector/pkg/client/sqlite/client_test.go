package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	client "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDBPath returns a unique shared-cache in-memory DB path per test,
// ensuring all connections in the pool see the same schema.
func testDBPath(t *testing.T) string {
	return fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
}

func TestNewClient(t *testing.T) {
	ctx := context.Background()
	params := client.DBParams{
		DBPath: testDBPath(t), // Use in-memory database for testing
	}

	dbClient, err := NewClient(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, dbClient)

	defer dbClient.CloseConnection(ctx)

	// Verify tables were created
	assert.True(t, dbClient.HasTable(ctx, "dump_pods"))
	assert.True(t, dbClient.HasTable(ctx, "timeline"))
	assert.True(t, dbClient.HasTable(ctx, "heap_dumps"))
	assert.True(t, dbClient.HasTable(ctx, "dump_objects_partitions"))
}

func TestPodOperations(t *testing.T) {
	ctx := context.Background()
	params := client.DBParams{
		DBPath: testDBPath(t),
	}

	dbClient, err := NewClient(ctx, params)
	require.NoError(t, err)
	defer dbClient.CloseConnection(ctx)

	// Create pod
	pod, created, err := dbClient.CreatePodIfNotExist(ctx,
		"default", "test-service", "test-pod", time.Now())
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotNil(t, pod)

	// Try to create same pod again
	pod2, created2, err := dbClient.CreatePodIfNotExist(ctx,
		"default", "test-service", "test-pod", pod.RestartTime)
	require.NoError(t, err)
	assert.False(t, created2)
	assert.Equal(t, pod.Id, pod2.Id)

	// Get pod count
	count, err := dbClient.GetPodsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Get pod by ID
	foundPod, err := dbClient.GetPodById(ctx, pod.Id)
	require.NoError(t, err)
	assert.Equal(t, pod.Id, foundPod.Id)
	assert.Equal(t, pod.Namespace, foundPod.Namespace)
}

func TestTimelineOperations(t *testing.T) {
	ctx := context.Background()
	params := client.DBParams{
		DBPath: testDBPath(t),
	}

	dbClient, err := NewClient(ctx, params)
	require.NoError(t, err)
	defer dbClient.CloseConnection(ctx)

	now := time.Now().UTC().Truncate(time.Hour)

	// Create timeline
	timeline, created, err := dbClient.CreateTimelineIfNotExist(ctx, now)
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotNil(t, timeline)
	assert.Equal(t, model.RawStatus, timeline.Status)

	// Try to create same timeline again
	timeline2, created2, err := dbClient.CreateTimelineIfNotExist(ctx, now)
	require.NoError(t, err)
	assert.False(t, created2)
	assert.True(t, timeline.TsHour.Equal(timeline2.TsHour))

	// Update timeline status
	updatedTimeline, err := dbClient.UpdateTimelineStatus(ctx, now, model.ZippedStatus)
	require.NoError(t, err)
	assert.Equal(t, model.ZippedStatus, updatedTimeline.Status)

	// Find timeline
	foundTimeline, err := dbClient.FindTimeline(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, model.ZippedStatus, foundTimeline.Status)
}

func TestHeapDumpOperations(t *testing.T) {
	ctx := context.Background()
	params := client.DBParams{
		DBPath: testDBPath(t),
	}

	dbClient, err := NewClient(ctx, params)
	require.NoError(t, err)
	defer dbClient.CloseConnection(ctx)

	// Create a pod first
	pod, _, err := dbClient.CreatePodIfNotExist(ctx,
		"default", "test-service", "test-pod", time.Now())
	require.NoError(t, err)

	// Create heap dump
	now := time.Now()
	dumpInfo := model.DumpInfo{
		Pod: model.Pod{
			Id:          pod.Id,
			Namespace:   pod.Namespace,
			ServiceName: pod.ServiceName,
			PodName:     pod.PodName,
			RestartTime: pod.RestartTime,
		},
		CreationTime: now,
		FileSize:     1024,
		DumpType:     model.HeapDumpType,
	}

	heapDump, created, err := dbClient.CreateHeapDumpIfNotExist(ctx, dumpInfo)
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotNil(t, heapDump)

	// Get heap dumps count
	count, err := dbClient.GetHeapDumpsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Find heap dump
	foundDump, err := dbClient.FindHeapDump(ctx, heapDump.Handle)
	require.NoError(t, err)
	assert.Equal(t, heapDump.Handle, foundDump.Handle)
}

func TestStoreDumpsTransactionally(t *testing.T) {
	ctx := context.Background()
	params := client.DBParams{
		DBPath: testDBPath(t),
	}

	dbClient, err := NewClient(ctx, params)
	require.NoError(t, err)
	defer dbClient.CloseConnection(ctx)

	now := time.Now().Truncate(time.Hour)

	// Prepare test data
	heapDumps := []model.DumpInfo{
		{
			Pod: model.Pod{
				Id:          uuid.New(),
				Namespace:   "default",
				ServiceName: "test-service",
				PodName:     "test-pod-1",
				RestartTime: now.Add(-time.Hour),
			},
			CreationTime: now,
			FileSize:     1024,
			DumpType:     model.HeapDumpType,
		},
	}

	tdTopDumps := []model.DumpInfo{
		{
			Pod: model.Pod{
				Id:          uuid.New(),
				Namespace:   "default",
				ServiceName: "test-service",
				PodName:     "test-pod-2",
				RestartTime: now.Add(-time.Hour),
			},
			CreationTime: now,
			FileSize:     512,
			DumpType:     model.TdDumpType,
		},
	}

	// Store dumps transactionally
	result, err := dbClient.StoreDumpsTransactionally(ctx, heapDumps, tdTopDumps, now)
	require.NoError(t, err)

	// Verify results
	assert.Equal(t, int64(1), result.TimelinesCreated)
	assert.Equal(t, int64(2), result.PodsCreated)
	assert.Equal(t, int64(1), result.HeapDumpsInserted)
	assert.Equal(t, int64(1), result.TdTopDumpsInserted)

	// Verify data was stored
	podsCount, err := dbClient.GetPodsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), podsCount)

	heapDumpsCount, err := dbClient.GetHeapDumpsCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), heapDumpsCount)

	// Verify timeline was created
	timeline, err := dbClient.FindTimeline(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, model.RawStatus, timeline.Status)
}
