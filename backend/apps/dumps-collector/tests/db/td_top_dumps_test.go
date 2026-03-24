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

type TdTopDumpTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *TdTopDumpTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *TdTopDumpTestSuite) SetupTest() {
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *TdTopDumpTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
}

func (suite *TdTopDumpTestSuite) CreateTdTopDumpIfNotExist() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpInfo := model.DumpInfo{
		Pod:          *pod,
		CreationTime: time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC),
		FileSize:     100,
		DumpType:     model.TdDumpType,
	}

	tdDump1, isCreated, err := suite.db.CreateTdTopDumpIfNotExist(suite.ctx, dumpInfo)
	require.NoError(t, err)
	require.True(t, isCreated)
	require.Equal(t, pod.Id, tdDump1.PodId)
	require.Equal(t, dumpInfo.CreationTime, tdDump1.CreationTime)
	require.Equal(t, int64(100), tdDump1.FileSize)
	require.Equal(t, model.TdDumpType, tdDump1.DumpType)

	tdDump2, isCreated, err := suite.db.CreateTdTopDumpIfNotExist(suite.ctx, dumpInfo)
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, tdDump1, tdDump2)
}

func (suite *TdTopDumpTestSuite) TestInsertTdTopDumps() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 12, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 12, 00, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 12, 00, 01, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 12, 00, 02, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
	}

	curTime := time.Date(2024, 07, 12, 00, 00, 00, 00, time.UTC)
	tdTopDumps, err := suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)
	require.Equal(t, 3, len(tdTopDumps))
}

func (suite *TdTopDumpTestSuite) TestFindTdTopDump() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 14, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	curTime := time.Date(2024, 07, 14, 00, 00, 00, 00, time.UTC)
	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 14, 00, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 14, 00, 01, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 14, 00, 02, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
	}

	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	tdTopDumps, err := suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)

	foundDump, err := suite.db.FindTdTopDump(suite.ctx, pod.Id, dumpsInfo[0].CreationTime, dumpsInfo[0].DumpType)
	require.NoError(t, err)
	require.Equal(t, tdTopDumps[0], *foundDump)
}

func (suite *TdTopDumpTestSuite) TestSearchHeapDumps() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 14, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	curTime := time.Date(2024, 07, 16, 00, 00, 00, 00, time.UTC)
	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 16, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 16, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 16, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 16, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
	}

	tdTopDumps, err := suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)

	searchedDumps, err := suite.db.SearchTdTopDumps(suite.ctx, curTime, []uuid.UUID{pod.Id},
		time.Date(2024, 07, 16, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 16, 00, 15, 00, 00, time.UTC), model.TdDumpType)
	require.NoError(t, err)
	require.Equal(t, 1, len(searchedDumps))
	require.Contains(t, searchedDumps, tdTopDumps[0])
}

func (suite *TdTopDumpTestSuite) TestGetTdTopDumpsCount() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 28, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	curTime := time.Date(2024, 07, 28, 00, 00, 00, 00, time.UTC)
	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 28, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 28, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 28, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 28, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TopDumpType,
		},
	}

	_, err = suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)

	count, err := suite.db.GetTdTopDumpsCount(suite.ctx, curTime,
		time.Date(2024, 07, 28, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 28, 00, 15, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func (suite *TdTopDumpTestSuite) TestCalculateSummaryTdTopDumps() {
	t := suite.T()

	pod1, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "svc-0", "pod-0", time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	pod2, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-1", "svc-0", "pod-1", time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	curTime := time.Date(2024, 07, 18, 00, 00, 00, 00, time.UTC)
	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 18, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC),
			FileSize:     50,
			DumpType:     model.TopDumpType,
		},
		{
			Pod:          *pod1,
			CreationTime: time.Date(2024, 07, 18, 00, 20, 00, 00, time.UTC),
			FileSize:     50,
			DumpType:     model.TopDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 18, 00, 20, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC),
			FileSize:     50,
			DumpType:     model.TopDumpType,
		},
		{
			Pod:          *pod2,
			CreationTime: time.Date(2024, 07, 18, 00, 20, 00, 00, time.UTC),
			FileSize:     50,
			DumpType:     model.TopDumpType,
		},
	}

	_, err = suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)

	summaries, err := suite.db.CalculateSummaryTdTopDumps(suite.ctx, curTime, []uuid.UUID{pod1.Id},
		time.Date(2024, 07, 18, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 18, 00, 15, 00, 00, time.UTC))

	require.NoError(t, err)
	require.Equal(t, 1, len(summaries))
	require.Equal(t, pod1.Id, summaries[0].PodId)
	require.Equal(t, time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC), summaries[0].DateFrom)
	require.Equal(t, time.Date(2024, 07, 18, 00, 10, 00, 00, time.UTC), summaries[0].DateTo)
	require.Equal(t, int64(150), summaries[0].SumFileSize)

	summaries, err = suite.db.CalculateSummaryTdTopDumps(suite.ctx, curTime, []uuid.UUID{pod1.Id, pod2.Id},
		time.Date(2024, 07, 18, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 18, 23, 00, 0, 00, time.UTC))

	require.NoError(t, err)
	require.Equal(t, 2, len(summaries))
	require.Equal(t, int64(300), summaries[0].SumFileSize)
	require.Equal(t, int64(300), summaries[1].SumFileSize)
}

func (suite *TdTopDumpTestSuite) TestRemoveHeapDumps() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	curTime := time.Date(2024, 07, 22, 00, 00, 00, 00, time.UTC)
	_, _, err = suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 00, 10, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
		{
			Pod:          *pod,
			CreationTime: time.Date(2024, 07, 22, 00, 12, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.TdDumpType,
		},
	}

	dumps, err := suite.db.InsertTdTopDumps(suite.ctx, curTime, dumpsInfo)
	require.NoError(t, err)

	removedDumps, err := suite.db.RemoveOldTdTopDumps(suite.ctx, curTime, time.Date(2024, 07, 22, 00, 11, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(removedDumps))
	require.Contains(t, removedDumps, dumps[0])

	foundDump, err := suite.db.FindTdTopDump(suite.ctx, pod.Id, dumps[0].CreationTime, dumps[0].DumpType)
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundDump)
}

func TestTdTopDumpTestSuite(t *testing.T) {
	suite.Run(t, new(TdTopDumpTestSuite))
}

