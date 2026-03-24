//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/maintenance"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DBCreationTestSuite struct {
	suite.Suite
	pg  *helpers.PostgresContainer
	job maintenance.MaintenanceJob
	ctx context.Context
}

func (suite *DBCreationTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)

	suite.pg = helpers.CreatePgContainer(suite.ctx)
	suite.job = maintenance.MaintenanceJob{
		Postgres:    suite.pg.Client,
		MinioClient: nil, // minio is not used for creation job
		JobConfig: &config.JobConfig{
			TempTableCreation: 1,
		},
		InvertedIndexConfig: &index.InvertedIndexConfig{
			Granularity: time.Hour,
			Lifetime:    pg.InvertedIndexLifetime,
			Prefixes:    []string{"requestid", "traceid"},
		},
	}
}

func (suite *DBCreationTestSuite) TestCreateTablesForSpecifiedTimeRange() {
	t := suite.T()
	ts := time.Date(2024, 5, 23, 0, 0, 0, 0, time.UTC)

	creationJob, err := maintenance.NewTempTablesCreationJob(suite.ctx, &suite.job, ts)
	require.NoError(t, err)

	err = creationJob.Execute(suite.ctx)
	require.NoError(t, err)

	expectedTableNames := []string{
		"calls_1716422400", "calls_1716422700", "calls_1716423000", "calls_1716423300", "calls_1716423600", "calls_1716423900", "calls_1716424200", "calls_1716424500", "calls_1716424800", "calls_1716425100", "calls_1716425400", "calls_1716425700", "calls_1716426000",
		"traces_1716422400", "traces_1716422700", "traces_1716423000", "traces_1716423300", "traces_1716423600", "traces_1716423900", "traces_1716424200", "traces_1716424500", "traces_1716424800", "traces_1716425100", "traces_1716425400", "traces_1716425700", "traces_1716426000",
		"suspend_1716422400", "suspend_1716422700", "suspend_1716423000", "suspend_1716423300", "suspend_1716423600", "suspend_1716423900", "suspend_1716424200", "suspend_1716424500", "suspend_1716424800", "suspend_1716425100", "suspend_1716425400", "suspend_1716425700", "suspend_1716426000",
		"dump_objects_1716422400", "dump_objects_1716426000",
		"i_requestid_1716422400", "i_traceid_1716422400", "i_requestid_1716426000", "i_traceid_1716426000"}
	tempTables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, ts, ts.Add(60*time.Minute))
	require.NoError(t, err)
	require.Equal(t, len(expectedTableNames), len(tempTables))
	for _, table := range tempTables {
		require.Contains(t, expectedTableNames, table.TableName, "found unexpected table with name %s", table.TableName)
		require.Equal(t, model.TableStatusReady, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
	}

	for _, tableName := range expectedTableNames {
		_, _, _, err := suite.pg.Client.GetTableMetadata(suite.ctx, tableName)
		require.NoError(t, err)
	}
}

func (suite *DBCreationTestSuite) TestCreateTablesWhenSomeOfThemAlreadyExists() {
	t := suite.T()
	ts := time.Date(2024, 5, 23, 0, 0, 0, 0, time.UTC)

	existedTables, err := suite.pg.Client.CreateTempTables(suite.ctx, ts)
	require.NoError(t, err)
	for _, existedTable := range existedTables {
		existedTable.Status = model.TableStatusReady
		err = suite.pg.Client.UpdateTempTableInventory(suite.ctx, *existedTable)
		require.NoError(t, err)
	}

	creationJob, err := maintenance.NewTempTablesCreationJob(suite.ctx, &suite.job, ts)
	require.NoError(t, err)

	err = creationJob.Execute(suite.ctx)
	require.NoError(t, err)

	expectedTableNames := []string{
		"calls_1716422400", "calls_1716422700", "calls_1716423000", "calls_1716423300", "calls_1716423600", "calls_1716423900", "calls_1716424200", "calls_1716424500", "calls_1716424800", "calls_1716425100", "calls_1716425400", "calls_1716425700", "calls_1716426000",
		"traces_1716422400", "traces_1716422700", "traces_1716423000", "traces_1716423300", "traces_1716423600", "traces_1716423900", "traces_1716424200", "traces_1716424500", "traces_1716424800", "traces_1716425100", "traces_1716425400", "traces_1716425700", "traces_1716426000",
		"suspend_1716422400", "suspend_1716422700", "suspend_1716423000", "suspend_1716423300", "suspend_1716423600", "suspend_1716423900", "suspend_1716424200", "suspend_1716424500", "suspend_1716424800", "suspend_1716425100", "suspend_1716425400", "suspend_1716425700", "suspend_1716426000",
		"dump_objects_1716422400", "dump_objects_1716426000",
		"i_requestid_1716422400", "i_traceid_1716422400", "i_requestid_1716426000", "i_traceid_1716426000"}
	tempTables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, ts, ts.Add(60*time.Minute))
	require.NoError(t, err)
	require.Equal(t, len(expectedTableNames), len(tempTables))
	for _, table := range tempTables {
		require.Contains(t, expectedTableNames, table.TableName, "found unexpected table with name %s", table.TableName)
		require.Equal(t, model.TableStatusReady, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
	}

	for _, tableName := range expectedTableNames {
		_, _, _, err := suite.pg.Client.GetTableMetadata(suite.ctx, tableName)
		require.NoError(t, err)
	}
}

func (suite *DBCreationTestSuite) TestCreateTablesWhenSomeOfThemAlreadyExistsWithUnexpectedStatus() {
	t := suite.T()
	ts := time.Date(2024, 5, 23, 0, 0, 0, 0, time.UTC)

	_, err := suite.pg.Client.CreateTempTables(suite.ctx, ts)
	require.NoError(t, err)

	creationJob, err := maintenance.NewTempTablesCreationJob(suite.ctx, &suite.job, ts)
	require.NoError(t, err)

	err = creationJob.Execute(suite.ctx)
	require.NoError(t, err)

	expectedTableNames := []string{
		"calls_1716422400", "calls_1716422700", "calls_1716423000", "calls_1716423300", "calls_1716423600", "calls_1716423900", "calls_1716424200", "calls_1716424500", "calls_1716424800", "calls_1716425100", "calls_1716425400", "calls_1716425700", "calls_1716426000",
		"traces_1716422400", "traces_1716422700", "traces_1716423000", "traces_1716423300", "traces_1716423600", "traces_1716423900", "traces_1716424200", "traces_1716424500", "traces_1716424800", "traces_1716425100", "traces_1716425400", "traces_1716425700", "traces_1716426000",
		"suspend_1716422400", "suspend_1716422700", "suspend_1716423000", "suspend_1716423300", "suspend_1716423600", "suspend_1716423900", "suspend_1716424200", "suspend_1716424500", "suspend_1716424800", "suspend_1716425100", "suspend_1716425400", "suspend_1716425700", "suspend_1716426000",
		"dump_objects_1716422400", "dump_objects_1716426000",
		"i_requestid_1716422400", "i_traceid_1716422400", "i_requestid_1716426000", "i_traceid_1716426000"}
	tempTables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, ts, ts.Add(60*time.Minute))
	require.NoError(t, err)
	require.Equal(t, len(expectedTableNames), len(tempTables))
	for _, table := range tempTables {
		require.Contains(t, expectedTableNames, table.TableName, "found unexpected table with name %s", table.TableName)
		if ts.Equal(table.StartTime) {
			require.Equal(t, model.TableStatusCreating, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
		} else {
			require.Equal(t, model.TableStatusReady, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
		}
	}

	for _, tableName := range expectedTableNames {
		_, _, _, err := suite.pg.Client.GetTableMetadata(suite.ctx, tableName)
		require.NoError(t, err)
	}
}

func (suite *DBCreationTestSuite) TearDownTest() {
	if err := suite.pg.CleanUpAllTempTables(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error cleaning up temp tables")
		suite.FailNow("tear down test")
	}
}

func (suite *DBCreationTestSuite) TearDownSuite() {
	if err := suite.pg.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating pg container")
		suite.FailNow("tear down")
	}
}

func TestDBCreationTestSuite(t *testing.T) {
	suite.Run(t, new(DBCreationTestSuite))
}

