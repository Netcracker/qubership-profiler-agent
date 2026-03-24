//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type HeapDumpTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DbClient
}

func (suite *HeapDumpTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *HeapDumpTestSuite) SetupTest() {
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *HeapDumpTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
}

func (suite *HeapDumpTestSuite) TestCreateHeapDumpIfNotExist() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp-1721606400000"
	restartTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpInfo := model.DumpInfo{
		Pod:          *pod,
		CreationTime: time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC),
		FileSize:     100,
		DumpType:     model.HeapDumpType,
	}

	heapDump, isCreated, err := suite.db.CreateHeapDumpIfNotExist(suite.ctx, dumpInfo)
	require.NoError(t, err)
	require.True(t, isCreated)
	require.Equal(t, "test-pod-78c95d6fc6-wfjkp-1721606400000-heap-1721606400000", heapDump.Handle)
	require.Equal(t, pod.Id, heapDump.PodId)
	require.Equal(t, dumpInfo.CreationTime, heapDump.CreationTime)
	require.Equal(t, int64(100), heapDump.FileSize)

	heapDump, isCreated, err = suite.db.CreateHeapDumpIfNotExist(suite.ctx, dumpInfo)
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, "test-pod-78c95d6fc6-wfjkp-1721606400000-heap-1721606400000", heapDump.Handle)
}

func (suite *HeapDumpTestSuite) TestInsertHeapDumps() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 01, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 02, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	heapDumps, err := suite.db.InsertHeapDumps(suite.ctx, dumpsInfo)
	require.NoError(t, err)
	require.Equal(t, 3, len(heapDumps))

	heapDumps, err = suite.db.InsertHeapDumps(suite.ctx, dumpsInfo[0:1])
	require.ErrorContains(t, err, "duplicate key value violates unique constraint \"heap_dumps_pkey\"")
	require.Nil(t, heapDumps)
}

func (suite *HeapDumpTestSuite) TestFindHeapDump() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp-1721606400000"
	restartTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	heapDumps, err := suite.db.InsertHeapDumps(suite.ctx, dumpsInfo)
	require.NoError(t, err)

	foundHeapDump, err := suite.db.FindHeapDump(suite.ctx, "test-pod-78c95d6fc6-wfjkp-1721606400000-heap-1721617200000")
	require.NoError(t, err)
	require.Equal(t, heapDumps[0], *foundHeapDump)

	foundHeapDump, err = suite.db.FindHeapDump(suite.ctx, "uexist-handle")
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundHeapDump)
}

func (suite *HeapDumpTestSuite) TestSearchHeapDumps() {
	t := suite.T()

	pod1, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "svc-0", "pod-0", time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	pod2, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-1", "svc-0", "pod-1", time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 22, 04, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 22, 04, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	heapDumps, err := suite.db.InsertHeapDumps(suite.ctx, dumpsInfo)
	require.NoError(t, err)

	searchedHheapDumps, err := suite.db.SearchHeapDumps(suite.ctx,
		[]uuid.UUID{pod1.Id},
		time.Date(2024, 07, 22, 02, 20, 00, 00, time.UTC),
		time.Date(2024, 07, 22, 03, 10, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(searchedHheapDumps))
	require.Contains(t, searchedHheapDumps, heapDumps[0])

	searchedHheapDumps, err = suite.db.SearchHeapDumps(suite.ctx,
		[]uuid.UUID{pod1.Id, pod2.Id},
		time.Date(2024, 07, 22, 02, 20, 00, 00, time.UTC),
		time.Date(2024, 07, 22, 03, 10, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(searchedHheapDumps))
	require.Contains(t, searchedHheapDumps, heapDumps[0])
	require.Contains(t, searchedHheapDumps, heapDumps[2])
}

func (suite *HeapDumpTestSuite) TestRemoveHeapDumps() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 21, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 21, 10, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 21, 12, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	heapDumps, err := suite.db.InsertHeapDumps(suite.ctx, dumpsInfo)
	require.NoError(t, err)

	remodedHeapDumps, err := suite.db.RemoveOldHeapDumps(suite.ctx, time.Date(2024, 07, 21, 11, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(remodedHeapDumps))
	require.Contains(t, remodedHeapDumps, heapDumps[0])

	foundHeapDump, err := suite.db.FindHeapDump(suite.ctx, heapDumps[0].Handle)
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundHeapDump)
}

func TestHeapDumpTestSuite(t *testing.T) {
	suite.Run(t, new(HeapDumpTestSuite))
}

