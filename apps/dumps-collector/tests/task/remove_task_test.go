//go:build integration

package task_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RemoveTaskTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *RemoveTaskTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RemoveTaskTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *RemoveTaskTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RemoveTaskTestSuite) TestWrongParameters() {
	t := suite.T()

	removeTask, err := task.NewRemoveTask(helpers.TestBaseDir, nil)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask("unexist-dir", suite.db)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask("insert_task_test.go", suite.db)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, removeTask)
}

// TestFullRun verifies the full execution flow of a remove task.
// It first runs RescanTask to populate the database, then RemoveTask with a cutoff at 2024-07-31 23:00.
// It checks that:
//   - only the 2024-08 directory remains in PV
//   - only 1 timeline (2024-08-01 00:00) remains in the database
//   - 2 pods are preserved
//   - 1 heap dump is present for 2024-08-01 00:01:42 time
//   - 8 top/thread dumps are preserved for the 2024-08-01 00 hour
func (suite *RemoveTaskTestSuite) TestFullRun() {
	t := suite.T()

	// Rescan removeTask to add entities to db
	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	removeTask, err := task.NewRemoveTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = removeTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Check that only the 2024-08 directory exists in PV
	yearDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024")
	entries, err := os.ReadDir(yearDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.Equal(t, "08", entries[0].Name())

	expectedTimeline := model.Timeline{
		Status: model.RawStatus,
		TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
	}

	// There should be only 1 timeline for 2024-08-01 00 hour
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(timelines))
	require.Contains(t, timelines, expectedTimeline)

	// There should be 2 pods in 2024-08 month:
	// test-service-1-5cbcd847d-l2t7t_1719318147399 and test-service-2-5cbcd847d-l2t7t_1719318147399
	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))

	podIds := make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
	}

	// There should be 1 heap dump for 2024-08-01 00:01:42
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(heapDumps))

	// There should be 8 thread/top dumps for 2024-08 month
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, timelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)
}

func TestRemoveTaskTestSuite(t *testing.T) {
	suite.Run(t, new(RemoveTaskTestSuite))
}
