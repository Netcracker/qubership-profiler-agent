//go:build integration

package integration

import (
	"reflect"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	"github.com/stretchr/testify/require"
)

func (suite *PGTestSuite) TestGetTempTablesNames() {
	t := suite.T()
	tableNames, err := suite.pg.Client.GetTempTablesNames(suite.ctx)
	require.NoError(t, err)
	tables := []string{"calls_1712102100", "calls_1712102400", "calls_1712102700",
		"dump_objects_1712098800", "dump_objects_1712102400",
		"traces_1712102100", "traces_1712102400", "traces_1712102700",
		"suspend_1712102100", "suspend_1712102400", "suspend_1712102700",
		"i_requestid_1712098800", "i_requestid_1712102400",
		"i_traceid_1712098800", "i_traceid_1712102400"}
	require.Equal(t, len(tables), len(tableNames))

	require.ElementsMatch(t, tables, tableNames)
}

func (suite *PGTestSuite) TestGetTempTableByStatusAndStartTimeBetween() {
	t := suite.T()
	fromTs := suite.timestamp.Add(-5 * time.Minute)
	toTs := suite.timestamp.Add(5 * time.Minute)
	tables, err := suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusCreating, fromTs, toTs)

	tableNames := []string{"calls_1712102100", "calls_1712102400", "calls_1712102700",
		"dump_objects_1712102400",
		"traces_1712102100", "traces_1712102400", "traces_1712102700",
		"suspend_1712102100", "suspend_1712102400", "suspend_1712102700",
		"i_requestid_1712102400",
		"i_traceid_1712102400"}
	require.NoError(t, err)
	require.Equal(t, len(tableNames), len(tables))
	for _, table := range tables {
		require.Contains(t, tableNames, table.TableName, "found unexpected table with name %s", table.TableName)
		require.Equal(t, model.TableStatusCreating, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
	}

	fromTs = suite.timestamp.Add(-2 * time.Minute)
	toTs = suite.timestamp.Add(2 * time.Minute)
	tables, err = suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusCreating, fromTs, toTs)
	tableNames = []string{"calls_1712102400", "dump_objects_1712102400", "traces_1712102400", "suspend_1712102400", "i_requestid_1712102400", "i_traceid_1712102400"}
	require.NoError(t, err)
	require.Equal(t, len(tableNames), len(tables))
	for _, table := range tables {
		require.Contains(t, tableNames, table.TableName, "found unexpected table with name %s", table.TableName)
		require.Equal(t, model.TableStatusCreating, table.Status, "found unexpected table status %s for table name %s", table.Status, table.TableName)
	}

	fromTs = suite.timestamp.Add(10 * time.Minute)
	toTs = suite.timestamp.Add(20 * time.Minute)
	tables, err = suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusCreating, fromTs, toTs)
	require.NoError(t, err)
	require.Equal(t, 0, len(tables), "found unexpected tables %v", reflect.ValueOf(tables).MapKeys())

	fromTs = suite.timestamp.Add(-2 * time.Minute)
	toTs = suite.timestamp.Add(2 * time.Minute)
	tables, err = suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusReady, fromTs, toTs)
	require.NoError(t, err)
	require.Equal(t, 0, len(tables), "found unexpected tables %v", reflect.ValueOf(tables).MapKeys())
}

func (suite *PGTestSuite) TestGetTempTableByStartTimeBetween() {
	t := suite.T()
	fromTs := suite.timestamp.Add(-5 * time.Minute)
	toTs := suite.timestamp.Add(5 * time.Minute)
	tables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, fromTs, toTs)
	require.NoError(t, err)

	tableNames := []string{"calls_1712102100", "calls_1712102400", "calls_1712102700",
		"dump_objects_1712102400",
		"traces_1712102100", "traces_1712102400", "traces_1712102700",
		"suspend_1712102100", "suspend_1712102400", "suspend_1712102700",
		"i_requestid_1712102400",
		"i_traceid_1712102400"}
	require.Equal(t, len(tableNames), len(tables))
	for _, table := range tables {
		require.Contains(t, tableNames, table.TableName, "found unexpected table with name %s", table.TableName)
	}

	fromTs = suite.timestamp.Add(-2 * time.Minute)
	toTs = suite.timestamp.Add(2 * time.Minute)
	tables, err = suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, fromTs, toTs)
	tableNames = []string{"calls_1712102400", "dump_objects_1712102400", "traces_1712102400", "suspend_1712102400", "i_requestid_1712102400", "i_traceid_1712102400"}
	require.NoError(t, err)
	require.Equal(t, len(tableNames), len(tables))
	for _, table := range tables {
		require.Contains(t, tableNames, table.TableName, "found unexpected table with name %s", table.TableName)
	}

	fromTs = suite.timestamp.Add(10 * time.Minute)
	toTs = suite.timestamp.Add(20 * time.Minute)
	tables, err = suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, fromTs, toTs)
	require.NoError(t, err)
	require.Equal(t, 0, len(tables), "found unexpected tables %v", reflect.ValueOf(tables).MapKeys())
}

func (suite *PGTestSuite) TestInsertTempTableInventory() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 15, 0, 0, 0, 0, time.Local)
	table := inventory.TempTableInfo{
		Uuid:           uuid,
		StartTime:      ts,
		EndTime:        ts.Add(pg.TempTableLifetime),
		Status:         model.TableStatusCreating,
		Type:           model.TableCalls,
		TableName:      pg.CallsTable(ts),
		CreatedTime:    ts,
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}

	err := suite.pg.Client.InsertTempTableInventory(suite.ctx, table)
	require.NoError(t, err)

	tables, err := suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusCreating, ts, ts)
	require.NoError(t, err)
	require.Contains(t, tables, table.TableName)
	require.Equal(t, table, *tables[table.TableName])

	uuid = common.RandomUuid()
	table = inventory.TempTableInfo{
		Uuid:           uuid,
		StartTime:      ts,
		EndTime:        ts.Add(pg.TempTableLifetime),
		Status:         model.TableStatusCreating,
		Type:           model.TableCalls,
		TableName:      pg.CallsTable(ts),
		CreatedTime:    ts,
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}

	err = suite.pg.Client.InsertTempTableInventory(suite.ctx, table)
	require.Errorf(t, err, "duplicate key value violates unique constraint")
}

func (suite *PGTestSuite) TestUpdateTempTableInventory() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 16, 0, 0, 0, 0, time.Local)
	table := inventory.TempTableInfo{
		Uuid:           uuid,
		StartTime:      ts,
		EndTime:        ts.Add(pg.TempTableLifetime),
		Status:         model.TableStatusCreating,
		Type:           model.TableCalls,
		TableName:      pg.CallsTable(ts),
		CreatedTime:    ts,
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}

	err := suite.pg.Client.InsertTempTableInventory(suite.ctx, table)
	require.NoError(t, err)

	table.Status = model.TableStatusReady
	table.RowsCount = 1
	err = suite.pg.Client.UpdateTempTableInventory(suite.ctx, table)
	require.NoError(t, err)

	tables, err := suite.pg.Client.GetTempTableByStatusAndStartTimeBetween(suite.ctx, model.TableStatusReady, ts, ts)
	require.NoError(t, err)
	require.Contains(t, tables, table.TableName)
	require.Equal(t, table, *tables[table.TableName])
}

func (suite *PGTestSuite) TestRemoveTempTableInventory() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 17, 0, 0, 0, 0, time.Local)
	table := inventory.TempTableInfo{
		Uuid:           uuid,
		StartTime:      ts,
		EndTime:        ts.Add(pg.TempTableLifetime),
		Status:         model.TableStatusCreating,
		Type:           model.TableCalls,
		TableName:      pg.CallsTable(ts),
		CreatedTime:    ts,
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}

	err := suite.pg.Client.InsertTempTableInventory(suite.ctx, table)
	require.NoError(t, err)

	tables, err := suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Contains(t, tables, table.TableName)
	require.Equal(t, table, *tables[table.TableName])

	err = suite.pg.Client.RemoveTempTableInventory(suite.ctx, table.Uuid)
	require.NoError(t, err)

	tables, err = suite.pg.Client.GetTempTableByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Equal(t, 0, len(tables))
}

func (suite *PGTestSuite) TestInsertS3File() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 15, 0, 0, 0, 0, time.Local)
	durationRange := model.Durations.GetByName("0ms")
	file := inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour),
		"ns-0", durationRange, "ns-0-0ms.parquet", "2024/05/15/0/ns-0-0ms.parquet")
	file.RemoteStoragePath = "2024/05/15/0/ns-0-0ms.parquet"
	file.Services.AddList([]string{"service-0"})
	err := suite.pg.Client.InsertS3File(suite.ctx, *file)
	require.NoError(t, err)

	files, err := suite.pg.Client.GetS3FilesByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Contains(t, files, file.RemoteStoragePath)
	require.Equal(t, *file, *files[file.RemoteStoragePath])
	require.Equal(t, []string{"service-0"}, file.Services.List())

	file = inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour),
		"ns-0", durationRange, "ns-0-0ms.parquet", "2024/05/15/0/ns-0-0ms.parquet")
	file.RemoteStoragePath = "2024/05/15/0/ns-0-0ms.parquet"
	err = suite.pg.Client.InsertS3File(suite.ctx, *file)
	require.Errorf(t, err, "duplicate key value violates unique constraint")
}

func (suite *PGTestSuite) TestUpdateS3File() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 16, 0, 0, 0, 0, time.Local)
	durationRange := model.Durations.GetByName("0ms")
	file := inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour),
		"ns-0", durationRange, "ns-0-0ms.parquet", "2024/05/16/0/ns-0-0ms.parquet")
	file.RemoteStoragePath = "2024/05/16/0/ns-0-0ms.parquet"

	err := suite.pg.Client.InsertS3File(suite.ctx, *file)
	require.NoError(t, err)

	file.Status = model.FileCreated
	file.RowsCount = 1
	err = suite.pg.Client.UpdateS3File(suite.ctx, *file)
	require.NoError(t, err)

	files, err := suite.pg.Client.GetS3FilesByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Contains(t, files, file.RemoteStoragePath)
	require.Equal(t, *file, *files[file.RemoteStoragePath])
}

func (suite *PGTestSuite) TestRemoveS3File() {
	t := suite.T()
	uuid := common.RandomUuid()
	ts := time.Date(2024, 5, 17, 0, 0, 0, 0, time.Local)
	durationRange := model.Durations.GetByName("0ms")
	file := inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour),
		"ns-0", durationRange, "ns-0-0ms.parquet", "2024/05/17/0/ns-0-0ms.parquet")
	file.RemoteStoragePath = "2024/05/17/0/ns-0-0ms.parquet"

	err := suite.pg.Client.InsertS3File(suite.ctx, *file)
	require.NoError(t, err)

	files, err := suite.pg.Client.GetS3FilesByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Contains(t, files, file.RemoteStoragePath)
	require.Equal(t, *file, *files[file.RemoteStoragePath])

	err = suite.pg.Client.RemoveS3File(suite.ctx, file.Uuid)
	require.NoError(t, err)

	files, err = suite.pg.Client.GetS3FilesByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
}

func (suite *PGTestSuite) TestGetS3FileByType() {
	t := suite.T()
	ts := time.Date(2024, 5, 18, 0, 0, 0, 0, time.Local)
	namespace := "ns-0"

	for _, dr := range model.Durations.List {
		fileName := helpers.GetTestCallS3FileName(namespace, dr)
		filePath := helpers.GetTestS3FileRemotePath(fileName, ts)
		uuid := common.RandomUuid()
		file := inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour), "ns-0", &dr, fileName, filePath)
		file.RemoteStoragePath = filePath

		err := suite.pg.Client.InsertS3File(suite.ctx, *file)
		require.NoError(t, err)
	}

	for _, dumpType := range model.AllDumpTypes {
		uuid := common.RandomUuid()
		fileName := helpers.GetTestDumpS3FileName(namespace, dumpType)
		filePath := helpers.GetTestS3FileRemotePath(fileName, ts)
		file := inventory.PrepareDumpsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour), "ns-0", dumpType, fileName, filePath)
		file.RemoteStoragePath = filePath

		err := suite.pg.Client.InsertS3File(suite.ctx, *file)
		require.NoError(t, err)
	}

	// TODO: rework heap test, that it was fully supported in profiler
	uuid := common.RandomUuid()
	heapFileName := "heap.parquet"
	heapFilePath := helpers.GetTestS3FileRemotePath(heapFileName, ts)
	file := inventory.PrepareDumpsFileInfo(uuid, ts, ts, ts.Add(14*24*time.Hour), "ns-0", model.DumpTypeHeap, heapFileName, heapFilePath)
	file.RemoteStoragePath = heapFilePath
	file.Type = model.FileHeap
	err := suite.pg.Client.InsertS3File(suite.ctx, *file)
	require.NoError(t, err)

	for _, dr := range model.Durations.List {
		fileName := helpers.GetTestCallS3FileName(namespace, dr)
		filePath := helpers.GetTestS3FileRemotePath(fileName, ts)
		files, err := suite.pg.Client.GetCallsS3FilesByDurationRangeAndStartTimeBetween(suite.ctx, dr, ts, ts)
		require.NoError(t, err)
		require.Equal(t, 1, len(files))
		require.Contains(t, files, filePath)
	}

	for _, dumpType := range model.AllDumpTypes {
		fileName := helpers.GetTestDumpS3FileName(namespace, dumpType)
		filePath := helpers.GetTestS3FileRemotePath(fileName, ts)
		files, err := suite.pg.Client.GetDumpsS3FilesByTypeAndStartTimeBetween(suite.ctx, dumpType, ts, ts)
		require.NoError(t, err)
		require.Equal(t, 1, len(files))
		require.Contains(t, files, filePath)
	}

	files, err := suite.pg.Client.GetHeapsS3FilesByStartTimeBetween(suite.ctx, ts, ts)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Contains(t, files, heapFilePath)
}
