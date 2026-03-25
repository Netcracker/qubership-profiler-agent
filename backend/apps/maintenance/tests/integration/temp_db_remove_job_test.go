//go:build integration

package integration

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/maintenance"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DBRemoveTestSuite struct {
	suite.Suite
	pg  *helpers.PostgresContainer
	job maintenance.MaintenanceJob
	ctx context.Context

	fromTs          time.Time
	toTs            time.Time
	existTempTables map[string]*inventory.TempTableInfo
}

func (suite *DBRemoveTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)

	suite.pg = helpers.CreatePgContainer(suite.ctx)
	suite.job = maintenance.MaintenanceJob{
		Postgres:    suite.pg.Client,
		MinioClient: nil, // minio is not used for remove job
		JobConfig: &config.JobConfig{
			TempTableRemoval: 1,
		},
		InvertedIndexConfig: &index.InvertedIndexConfig{
			Granularity: time.Hour,
			Lifetime:    pg.InvertedIndexLifetime,
			Prefixes:    []string{"requestid", "traceid"},
		},
	}
}

func (suite *DBRemoveTestSuite) SetupTest() {
	suite.fromTs = time.Date(2024, 5, 23, 0, 0, 0, 0, time.UTC)
	suite.toTs = time.Date(2024, 5, 23, 0, 10, 0, 0, time.UTC)
	var err error
	if suite.existTempTables, err = suite.pg.AddTempTables(suite.ctx, suite.fromTs, suite.toTs, model.TableStatusPersisted); err != nil {
		log.Error(suite.ctx, err, "error creating initial temp tables")
		suite.FailNow("setup sub test")
	}
}

func (suite *DBRemoveTestSuite) TestRemoveTablesForSpecifiedTimeRange() {
	// Data generated from 00:00 to 00:10
	// Now is 01:05
	// Data from 00:00 to 00:05 should be removed
	t := suite.T()
	ts := time.Date(2024, 5, 23, 1, 5, 0, 0, time.UTC)

	expectedTableNamesToExist := []string{"calls_1716423000", "calls_1716422700", "dump_objects_1716422400",
		"traces_1716423000", "traces_1716422700", "suspend_1716423000", "suspend_1716422700",
		"i_requestid_1716422400", "i_traceid_1716422400"}
	removeJob, err := maintenance.NewTempTablesRemoveJob(suite.ctx, &suite.job, ts)
	require.NoError(t, err)

	err = removeJob.Execute(suite.ctx)
	require.NoError(t, err)

	tempTables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, suite.fromTs, suite.toTs)
	require.NoError(t, err)
	require.Equal(t, len(expectedTableNamesToExist), len(tempTables))
	for _, table := range tempTables {
		require.Contains(t, expectedTableNamesToExist, table.TableName, "found unexpected table with name %s", table.TableName)
	}

	for _, tableInfo := range suite.existTempTables {
		if !slices.Contains(expectedTableNamesToExist, tableInfo.TableName) {
			_, _, _, err := suite.pg.Client.GetTableMetadata(suite.ctx, tableInfo.TableName)
			require.Errorf(t, err, "does not exist")
		}
	}
}

func (suite *DBRemoveTestSuite) TestRemoveTablesForSpecifiedTimeRangeWithUnexpectedStatus() {
	// Data generated from 00:00 to 00:10
	// Now is 01:05
	// Data from 00:00 to 00:05 should be removed
	t := suite.T()
	ts := time.Date(2024, 5, 23, 1, 5, 0, 0, time.UTC)

	unexpectedStatusTableNames := []string{"calls_1716422700", "dump_objects_1716422400", "traces_1716422700",
		"suspend_1716422400", "i_requestid_1716422400", "i_traceid_1716422400"}
	for _, tempTableName := range unexpectedStatusTableNames {
		tempTable := suite.existTempTables[tempTableName]
		tempTable.Status = model.TableStatusPersisting
		err := suite.pg.Client.UpdateTempTableInventory(suite.ctx, *tempTable)
		require.NoError(t, err)
	}

	expectedTableNamesToExist := []string{"calls_1716423000", "traces_1716423000", "suspend_1716423000", "suspend_1716422700"}
	expectedTableNamesToExist = append(expectedTableNamesToExist, unexpectedStatusTableNames...)
	removeJob, err := maintenance.NewTempTablesRemoveJob(suite.ctx, &suite.job, ts)
	require.NoError(t, err)

	err = removeJob.Execute(suite.ctx)
	require.NoError(t, err)

	tempTables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, suite.fromTs, suite.toTs)
	require.NoError(t, err)
	require.Equal(t, len(expectedTableNamesToExist), len(tempTables))
	for _, table := range tempTables {
		require.Contains(t, expectedTableNamesToExist, table.TableName, "found unexpected table with name %s", table.TableName)
	}

	for _, tableInfo := range suite.existTempTables {
		if !slices.Contains(expectedTableNamesToExist, tableInfo.TableName) {
			_, _, _, err := suite.pg.Client.GetTableMetadata(suite.ctx, tableInfo.TableName)
			require.Errorf(t, err, "does not exist")
		}
	}
}

func (suite *DBRemoveTestSuite) TearDownTest() {
	if err := suite.pg.CleanUpAllTempTables(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error cleaning up temp tables")
		suite.FailNow("tear down test")
	}
}

func (suite *DBRemoveTestSuite) TearDownSuite() {
	if err := suite.pg.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating pg container")
		suite.FailNow("tear down")
	}
}

func TestDBRemoveTestSuite(t *testing.T) {
	suite.Run(t, new(DBRemoveTestSuite))
}

