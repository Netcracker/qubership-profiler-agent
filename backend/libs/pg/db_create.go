package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg/queries"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/jackc/pgx/v5"
)

func (c *Client) InsertS3File(ctx context.Context, file inventory.S3FileInfo) error {
	startTime := time.Now()
	log.Debug(ctx, "[InsertS3File] S3 file name: %s", file.FileName)

	query := fmt.Sprintf(queries.InsertS3File, S3FilesTable)

	err := c.executeTransaction(ctx, query,
		file.Uuid.Val, file.StartTime, file.EndTime,
		file.Type, file.DumpType, file.Namespace, file.DurationRange,
		file.FileName, file.Status, file.Services.List(),
		file.CreatedTime, file.ApiVersion,
		file.RowsCount, file.FileSize,
		file.RemoteStoragePath, file.LocalFilePath,
	)
	if err != nil {
		log.Error(ctx, err, "[InsertS3File] Could not insert data to %s", S3FilesTable)
		return err
	}

	log.Debug(ctx, "[InsertS3File] done in %v", time.Since(startTime))
	return err
}

// InsertInvertedIndex persists the data to inverted indexes
func (c *Client) InsertInvertedIndex(ctx context.Context, tableTime time.Time, indexes *index.Map) error {
	startTime := time.Now()
	log.Debug(ctx, "[InsertInvertedIndex] data for %d unique parameters", indexes.ParametersCount())
	// the same order is not guaranteed for two iterations of the map, so we will use a list to get the result for a specific record
	pattern := "{value: %v, file_id: %v}"
	list := make([]string, 0, len(indexes.Indexes))
	batch := &pgx.Batch{}

	// indexes.Indexes updates in compactor Pod Job (see pkg/compactor/pod_job.go:77)
	for param, values := range indexes.Indexes {
		prefix, err := common.NormalizeParam(param)
		if err != nil {
			log.Error(ctx, err, "[InsertInvertedIndex] Could not normalize param %s", param)
			continue
		}

		// TODO: Should we check if the table exists in the database before using it?
		tableName := InvertedIndexTable(prefix, tableTime)

		query := fmt.Sprintf(queries.InsertInvertedIndex, tableName)

		for _, item := range values {
			// Convert file_id from a colon-separated hex string (e.g. "FE:7D:...") to plain hex format ("FE7D...").
			// This is required because Postgres expects UUID in a compact form without colons.
			batch.Queue(query, item.Value, strings.ReplaceAll(item.FileId, ":", ""))
			list = append(list, fmt.Sprintf(pattern, item.Value, item.FileId))
		}
	}

	results := c.conn.SendBatch(ctx, batch)
	defer results.Close()

	for _, item := range list {
		_, err := results.Exec()
		if err != nil {
			log.Error(ctx, err, "[InsertInvertedIndex] Unable to insert row to inverted index for %v", item)
			return err
		}
	}

	log.Debug(ctx, "[InsertInvertedIndex] insert %d in %v", len(list), time.Since(startTime))

	return results.Close()
}

// InsertTempTableInventory persists the given inventory.TempTableInfo into the temp_table_inventory table.
// Executes an INSERT query inside a transaction and logs execution time for debugging.
func (c *Client) InsertTempTableInventory(ctx context.Context, table inventory.TempTableInfo) error {
	startTime := time.Now()
	log.Debug(ctx, "[InsertTempTable] Temp table name: %s", table.TableName)

	query := fmt.Sprintf(queries.InsertTempTableInventory, TempTableInventoryTable)

	err := c.executeTransaction(ctx, query,
		table.Uuid.Val,
		table.StartTime, table.EndTime,
		table.Status, table.Type,
		table.TableName,
		table.CreatedTime,
		table.RowsCount,
		table.TableSize,
		table.TableTotalSize,
	)
	if err != nil {
		return fmt.Errorf("could not insert data to inventory: %v", err)
	}

	log.Debug(ctx, "[InsertTempTable] done in %v", time.Since(startTime))
	return nil
}

func (c *Client) InsertPod(ctx context.Context, pod model.PodInfo) error {
	startTime := time.Now()
	log.Trace(ctx, "[InsertPod] pod name: %s", pod.PodName)

	query := fmt.Sprintf(queries.InsertPod, PodsTable)
	err := c.executeTransaction(ctx, query,
		pod.PodId, pod.Namespace, pod.ServiceName, pod.PodName,
		pod.ActiveSince, pod.LastRestart, pod.LastActive,
		pod.Tags,
	)
	if err != nil {
		log.Error(ctx, err, "[InsertPod] Could not insert data to %s", PodsTable)
		return err
	}

	log.Debug(ctx, "[InsertPod] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertPodRestart(ctx context.Context, pod model.PodRestart) error {
	startTime := time.Now()
	log.Trace(ctx, "[InsertPodRestart] pod name: %s, restart time: %v", pod.PodName, pod.RestartTime)

	query := fmt.Sprintf(queries.InsertPodRestart, PodRestartsTable)
	err := c.executeTransaction(ctx, query,
		pod.PodId, pod.Namespace, pod.ServiceName, pod.PodName, pod.RestartTime,
		pod.ActiveSince, pod.LastActive)
	if err != nil {
		log.Error(ctx, err, "[InsertPodRestart] Could not insert data to %s", PodRestartsTable)
		return err
	}

	log.Debug(ctx, "[InsertPodRestart] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertParam(ctx context.Context, param model.Param) error {
	startTime := time.Now()
	log.Trace(ctx, "[InsertParam] param name: %s", param.ParamName)

	query := fmt.Sprintf(queries.InsertParam, ParamsTable)
	err := c.executeTransaction(ctx, query,
		param.PodId, param.PodName, param.RestartTime,
		param.ParamName, param.ParamIndex, param.ParamList, param.ParamOrder, param.Signature)

	if err != nil {
		log.Error(ctx, err, "[InsertParam] Could not insert data to %s", ParamsTable)
		return err
	}

	log.Debug(ctx, "[InsertParam] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertDictionary(ctx context.Context, dict model.Dictionary) error {
	startTime := time.Now()
	log.Trace(ctx, "[InsertDictionary] pod name: %s, restart time: %v", dict.PodName, dict.RestartTime)

	query := fmt.Sprintf(queries.InsertDictionary, DictionaryTable)
	err := c.executeTransaction(ctx, query,
		dict.PodId, dict.PodName, dict.RestartTime,
		dict.Position, dict.Tag)

	if err != nil {
		log.Error(ctx, err, "[InsertDictionary] Could not insert data to %s", DictionaryTable)
		return err
	}

	log.Debug(ctx, "[InsertDictionary] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertCall(ctx context.Context, call model.Call) error {
	startTime := time.Now()
	log.ExtraTrace(ctx, "[InsertCall] call: %s", call)

	tableName := CallsTable(call.Time)
	query := fmt.Sprintf(queries.InsertCall, tableName)
	err := c.executeTransaction(ctx, query,
		call.Time,
		call.CpuTime, call.WaitTime, call.MemoryUsed, call.Duration,
		call.NonBlocking, call.QueueWaitDuration, call.SuspendDuration,
		call.Calls, call.Transactions,
		call.LogsGenerated, call.LogsWritten,
		call.FileRead, call.FileWritten,
		call.NetRead, call.NetWritten,
		call.Namespace, call.ServiceName, call.PodName, call.RestartTime,
		call.Method, call.Params,
		call.TraceFileIndex, call.BufferOffset, call.RecordIndex)

	if err != nil {
		log.Error(ctx, err, "[InsertCall] Could not insert to %s", tableName)
		return err
	}

	log.Debug(ctx, "[InsertCall] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertTrace(ctx context.Context, t time.Time, trace model.Trace) error {
	startTime := time.Now()
	log.ExtraTrace(ctx, "[InsertTrace] trace: %s", trace)

	tableName := TracesTable(t)
	query := fmt.Sprintf(queries.InsertTrace, tableName)

	err := c.executeTransaction(ctx, query,
		trace.PodName, trace.RestartTime,
		trace.TraceFileIndex, trace.BufferOffset, trace.RecordIndex,
		trace.Trace)

	if err != nil {
		log.Trace(ctx, "[InsertTrace] Could not insert data to %s", tableName) // not to spam
	}
	log.Debug(ctx, "[InsertTrace] done in %v", time.Since(startTime))
	return err
}

func (c *Client) InsertDump(ctx context.Context, dump model.Dump) error {
	startTime := time.Now()
	log.Trace(ctx, "[InsertDump] dump: %s", dump)

	tableName := DumpsTable(dump.CreatedTime)
	query := fmt.Sprintf(queries.InsertDump, tableName)

	err := c.executeTransaction(ctx, query,
		dump.UUID.Val, dump.CreatedTime,
		dump.Namespace, dump.ServiceName, dump.PodName, dump.RestartTime, dump.PodType,
		dump.DumpType, dump.BytesSize, common.MapToJsonString(dump.Info),
		dump.BinaryData)
	if err != nil {
		log.Error(ctx, err, "[InsertDump] Could not insert data to %s", tableName)
	}

	log.Debug(ctx, "[InsertDump] done in %v", time.Since(startTime))
	return err
}
