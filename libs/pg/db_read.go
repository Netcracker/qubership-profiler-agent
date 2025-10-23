package pg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg/queries"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

const (
	CallsSliceInitCapacity = 1000
	DumpsSliceInitCapacity = 1000
)

func (c *Client) GetTempTableByStatusAndStartTimeBetween(ctx context.Context, status model.TableStatus, fromTs time.Time, toTs time.Time) (map[string]*inventory.TempTableInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetTempTableByStatusAndStartTimeBetween [status: %s, from: %v, to: %v] ", status, fromTs, toTs)

	tables := make(map[string]*inventory.TempTableInfo)
	query := fmt.Sprintf(queries.GetTempTableByStatusAndStartTimeBetween, TempTableInventoryTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, string(status), fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var table inventory.TempTableInfo
		var tableSize sql.NullInt64
		var totalTableSize sql.NullInt64
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &table.StartTime, &table.EndTime, &table.Status, &table.Type, &table.TableName, &table.CreatedTime, &table.RowsCount, &tableSize, &totalTableSize); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", TempTableInventoryTable)
			return nil, err
		}

		table.Uuid = common.ToUuid(uuidBytes)

		if tableSize.Valid {
			table.TableSize = tableSize.Int64
		}
		if totalTableSize.Valid {
			table.TableTotalSize = totalTableSize.Int64
		}

		tables[table.TableName] = &table
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetTempTableByStatusAndStartTimeBetween is finished. number of tables: %d. [Execution time - %v]", len(tables), time.Since(startTime))

	return tables, nil
}

func (c *Client) GetTempTableByStartTimeBetween(ctx context.Context, fromTs time.Time, toTs time.Time) (map[string]*inventory.TempTableInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetTempTableStartTimeBetween [from: %v, to: %v] ", fromTs, toTs)

	tables := make(map[string]*inventory.TempTableInfo)
	query := fmt.Sprintf(queries.GetInventoryByStartTimeBetween, TempTableInventoryTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "Execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var table inventory.TempTableInfo
		var tableSize sql.NullInt64
		var totalTableSize sql.NullInt64
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &table.StartTime, &table.EndTime, &table.Status, &table.Type, &table.TableName, &table.CreatedTime, &table.RowsCount, &tableSize, &totalTableSize); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", TempTableInventoryTable)
			return nil, err
		}

		table.Uuid = common.ToUuid(uuidBytes)

		if tableSize.Valid {
			table.TableSize = tableSize.Int64
		}
		if totalTableSize.Valid {
			table.TableTotalSize = totalTableSize.Int64
		}

		tables[table.TableName] = &table
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetTempTableByStartTimeBetween is finished. number of tables: %d. [Execution time - %v]", len(tables), time.Since(startTime))

	return tables, nil
}

func (c *Client) GetTempTableByEndTimeBetween(ctx context.Context, fromTs time.Time, toTs time.Time) (map[string]*inventory.TempTableInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetTempTableByEndTimeBetween [from: %v, to: %v] ", fromTs, toTs)

	tables := make(map[string]*inventory.TempTableInfo)
	query := fmt.Sprintf(queries.GetInventoryByEndTimeBetween, TempTableInventoryTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var table inventory.TempTableInfo
		var tableSize sql.NullInt64
		var totalTableSize sql.NullInt64
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &table.StartTime, &table.EndTime, &table.Status, &table.Type, &table.TableName, &table.CreatedTime, &table.RowsCount, &tableSize, &totalTableSize); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", TempTableInventoryTable)
			return nil, err
		}

		table.Uuid = common.ToUuid(uuidBytes)

		if tableSize.Valid {
			table.TableSize = tableSize.Int64
		}
		if totalTableSize.Valid {
			table.TableTotalSize = totalTableSize.Int64
		}

		tables[table.TableName] = &table
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetTempTableByEndTimeBetween is finished. number of tables: %d. [Execution time - %v]", len(tables), time.Since(startTime))

	return tables, nil
}

func (c *Client) GetUniqueNamespaces(ctx context.Context) ([]string, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetUniqueNamespaces")

	var namespaces []string
	nsQuery := fmt.Sprintf(queries.GetUniqueNamespaces, PodsTable)
	rows, err := c.conn.Query(ctx, nsQuery)
	if err != nil {
		log.Error(ctx, err, "problem during execution query for getting unique namespaces")
		return nil, err
	}

	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			log.Error(ctx, err, "problem with scanning the result when getting unique namespaces")
			return nil, err
		}

		namespaces = append(namespaces, namespace)
	}

	log.Debug(ctx, "GetUniqueNamespaces is finished. number of namespaces: %d. [Execution time - %v]", len(namespaces), time.Since(startTime))

	return namespaces, nil
}

func (c *Client) GetUniquePodsForNamespaceActiveAfter(ctx context.Context, namespace string, activeAfter time.Time) ([]*model.PodInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetUniquePodsForNamespaceActiveAfter for %s namespace actived after %v", namespace, activeAfter)

	var pods []*model.PodInfo
	query := fmt.Sprintf(queries.GetUniquePodsForNamespaceActiveAfter, PodsTable)
	rows, err := c.conn.Query(ctx, query, namespace, activeAfter)
	if err != nil {
		log.Error(ctx, err, "problem during execution query for getting unique pods")
		return nil, err
	}

	for rows.Next() {
		var pod model.PodInfo
		if err := rows.Scan(&pod.PodId, &pod.Namespace, &pod.ServiceName, &pod.PodName, &pod.ActiveSince, &pod.LastRestart, &pod.LastActive, &pod.Tags); err != nil {
			log.Error(ctx, err, "problem with scanning the result when getting unique pods")
			return nil, err
		}
		pods = append(pods, &pod)
	}

	log.Debug(ctx, "GetUniquePodsForNamespaceActiveAfter is finished. namespace: %s, number of pods: %d. [Execution time - %v]", namespace, len(pods), time.Since(startTime))
	return pods, nil
}

func (c *Client) GetUniquePodsForNamespaceActiveBefore(ctx context.Context, namespace string, activeBefore time.Time) ([]*model.PodInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetUniquePodsForNamespaceActiveBefore for %s namespace actived before %v", namespace, activeBefore)

	var pods []*model.PodInfo
	query := fmt.Sprintf(queries.GetUniquePodsForNamespaceActiveBefore, PodsTable)
	rows, err := c.conn.Query(ctx, query, namespace, activeBefore)
	if err != nil {
		log.Error(ctx, err, "problem during execution query for getting unique pods")
		return nil, err
	}

	for rows.Next() {
		var pod model.PodInfo
		if err := rows.Scan(&pod.PodId, &pod.Namespace, &pod.ServiceName, &pod.PodName, &pod.ActiveSince, &pod.LastRestart, &pod.LastActive, &pod.Tags); err != nil {
			log.Error(ctx, err, "problem with scanning the result when getting unique pods")
			return nil, err
		}
		pods = append(pods, &pod)
	}

	log.Debug(ctx, "GetUniquePodsForNamespaceActiveBefore is finished. namespace: %s, number of pods: %d. [Execution time - %v]", namespace, len(pods), time.Since(startTime))
	return pods, nil
}

func (c *Client) GetPodRestarts(ctx context.Context, namespace string, service string, podName string) ([]*model.PodRestart, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetPodRestarts for [%s/%s/%s]", namespace, service, podName)

	var podRestarts []*model.PodRestart
	query := fmt.Sprintf(queries.GetPodRestarts, PodRestartsTable)
	rows, err := c.conn.Query(ctx, query, namespace, service, podName)
	if err != nil {
		log.Error(ctx, err, "problem during execution query for getting pod restarts")
		return nil, err
	}

	for rows.Next() {
		var podRestart model.PodRestart
		if err := rows.Scan(&podRestart.PodId, &podRestart.Namespace, &podRestart.ServiceName, &podRestart.PodName, &podRestart.RestartTime, &podRestart.ActiveSince, &podRestart.LastActive); err != nil {
			log.Error(ctx, err, "problem with scanning the result when getting pod restarts")
			return nil, err
		}
		podRestarts = append(podRestarts, &podRestart)
	}

	log.Debug(ctx, "GetPodRestarts is finished for [%s/%s/%s], number of pod resarts: %d. [Execution time - %v]", namespace, service, podName, len(podRestarts), time.Since(startTime))
	return podRestarts, nil
}

func (c *Client) GetCallsTimeBetween(ctx context.Context, namespace string, ts time.Time) ([]*model.Call, error) {
	startTime := time.Now()
	log.ExtraTrace(ctx, "[GetCallsTimeBetween] Start for [%s]", namespace)

	var calls = make([]*model.Call, 0, CallsSliceInitCapacity)
	callTbName := CallsTable(ts)

	query := fmt.Sprintf(queries.GetCallsTimeBetween, callTbName)
	rows, err := c.conn.Query(ctx, query, namespace, ts.Add(10*time.Minute), ts.Add(-10*time.Minute))
	if err != nil {
		log.Error(ctx, err, "[GetCallsTimeBetween] could not execute query %v", query)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var call model.Call
		var str *string
		err = rows.Scan(
			&call.Time, &call.CpuTime, &call.WaitTime, &call.MemoryUsed, &call.Duration, &call.NonBlocking,
			&call.QueueWaitDuration, &call.SuspendDuration,
			&call.Calls, &call.Transactions, &call.LogsGenerated, &call.LogsWritten, &call.FileRead, &call.FileWritten,
			&call.NetRead, &call.NetWritten,
			&call.Namespace, &call.ServiceName, &call.PodName, &call.RestartTime,
			&call.Method, &str,
			&call.TraceFileIndex, &call.BufferOffset, &call.RecordIndex,
		)
		if err != nil {
			log.Error(ctx, err, "[GetCallsTimeBetween] problem during scan record from [%s] tables", callTbName)
			return nil, err
		}

		if str != nil {

			call.Params, err = jsonToMap[map[int][]string](*str)
			if err != nil {
				log.Error(ctx, err, "[GetCallsTimeBetween] problem during scan record from [%s] tables", callTbName)
				return nil, err
			}
		}
		calls = append(calls, &call)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "[GetCallsTimeBetween] Found %d calls in %v", len(calls), time.Since(startTime))

	return calls, nil
}

func (c *Client) GetCallsWithTraceTimeBetween(ctx context.Context,
	namespace string, pod *model.PodInfo, callTbName, traceTbName string,
	upperBound, lowerBound time.Time) ([]*model.CallWithTraces, error) {

	startTime := time.Now()
	log.ExtraTrace(ctx, "[GetCallsWithTraceTimeBetween] Start for [%s/%s/%s]", namespace, pod.ServiceName, pod.PodName)

	var calls = make([]*model.CallWithTraces, 0, CallsSliceInitCapacity)

	query := fmt.Sprintf(queries.GetCallsWithTraceTimeBetween, callTbName, traceTbName)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "[GetCallsWithTraceTimeBetween] Execute query: %v \n Unit: [%v : %v : %v]. Tables: [%s : %s]. Bounds [%v:%v)",
			strings.ReplaceAll(query, "\n", " "),
			namespace, pod.ServiceName, pod.PodName, callTbName, traceTbName, lowerBound, upperBound)
	}

	rows, err := c.conn.Query(ctx, query, namespace, pod.ServiceName, pod.PodName, upperBound, lowerBound)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var call model.CallWithTraces
		var str string
		err = rows.Scan(
			&call.Time, &call.CpuTime, &call.WaitTime, &call.MemoryUsed, &call.Duration, &call.NonBlocking,
			&call.QueueWaitDuration, &call.SuspendDuration,
			&call.Calls, &call.Transactions, &call.LogsGenerated, &call.LogsWritten, &call.FileRead, &call.FileWritten,
			&call.NetRead, &call.NetWritten,
			&call.Namespace, &call.ServiceName, &call.PodName, &call.RestartTime,
			&call.Method, &str,
			&call.TraceFileIndex, &call.BufferOffset, &call.RecordIndex, &call.Trace,
		)
		if err != nil {
			log.Error(ctx, err, "[GetCallsWithTraceTimeBetween] problem during scan record from [%s/%s] tables", callTbName, traceTbName)
			return nil, err
		}

		call.Params, err = jsonToMap[map[int][]string](str)
		if err != nil {
			log.Error(ctx, err, "[GetCallsWithTraceTimeBetween] problem during scan record from [%s/%s] tables", callTbName, traceTbName)
			return nil, err
		}
		calls = append(calls, &call)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "[GetCallsWithTraceTimeBetween] Found %d calls in %v", len(calls), time.Since(startTime))

	return calls, nil
}

func (c *Client) GetDumpsTimeBetween(ctx context.Context, pod *model.PodInfo, dumpsTbName string, upperBound, lowerBound time.Time) ([]*model.Dump, error) {
	startTime := time.Now()
	log.ExtraTrace(ctx, "[GetDumpsTimeBetween] Start for [%s/%s/%s]", pod.Namespace, pod.ServiceName, pod.PodName)

	var dumps = make([]*model.Dump, 0, DumpsSliceInitCapacity)
	query := fmt.Sprintf(queries.GetDumpsTimeBetween, dumpsTbName)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "[GetDumpsTimeBetween] Execute query: %v \n Unit: [%v : %v : %v]. Table: [%s]. Bounds [%v:%v)",
			strings.ReplaceAll(query, "\n", " "),
			pod.Namespace, pod.ServiceName, pod.PodName, dumpsTbName, lowerBound, upperBound)
	}

	rows, err := c.conn.Query(ctx, query, pod.Namespace, pod.ServiceName, pod.PodName, upperBound, lowerBound)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var dump model.Dump
		var uuidBytes [16]byte

		err := rows.Scan(&uuidBytes, &dump.CreatedTime, &dump.Namespace, &dump.ServiceName, &dump.PodName, &dump.PodType, &dump.RestartTime, &dump.DumpType, &dump.BytesSize, &dump.Info, &dump.BinaryData)
		if err != nil {
			log.Error(ctx, err, "[GetDumpsTimeBetween] problem during scan record from [%s] tables", dumpsTbName)
			return nil, err
		}
		dump.UUID = common.ToUuid(uuidBytes)

		dumps = append(dumps, &dump)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "[GetDumpsTimeBetween] Found %d dumps in %v", len(dumps), time.Since(startTime))
	return dumps, nil
}

func (c *Client) GetTagByPosition(ctx context.Context, position int) (string, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetTagByPosition for position %d", position)

	var tag string
	query := fmt.Sprintf(queries.GetTagByPosition, DictionaryTable)
	row := c.conn.QueryRow(ctx, query, position)

	if err := row.Scan(&tag); err != nil {
		log.Error(ctx, err, "problem with scanning the result when getting tag by position %d from table %s", position, DictionaryTable)
		return "", err
	}

	log.Debug(ctx, "GetTagByPosition is finished. Tag is %s. [Execution time - %v]", tag, time.Since(startTime))
	return tag, nil
}

func (c *Client) GetTableMetadata(ctx context.Context, tbName string) (rowsCount int, size int64, totalSize int64, err error) {
	startTime := time.Now()
	log.Debug(ctx, "Start getting size for %s", tbName)

	query := fmt.Sprintf(queries.GetRowsCount, tbName)
	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v", strings.ReplaceAll(query, "\n", " "))
	}

	err = c.conn.QueryRow(ctx, query).Scan(&rowsCount)
	if err != nil {
		log.Error(ctx, err, "problem during getting the number of rows from %s", tbName)
	}

	query = fmt.Sprintf(queries.GetTableSize, tbName)
	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v", strings.ReplaceAll(query, "\n", " "))
	}

	err = c.conn.QueryRow(ctx, query).Scan(&size)
	if err != nil {
		log.Error(ctx, err, "problem during getting size for %s", tbName)
	}

	query = fmt.Sprintf(queries.GetTotalTableSize, tbName)
	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v", strings.ReplaceAll(query, "\n", " "))
	}

	err = c.conn.QueryRow(ctx, query).Scan(&totalSize)
	if err != nil {
		log.Error(ctx, err, "problem during getting total size for %s", tbName)
	}

	log.Debug(ctx, "GetTableSize is finished. [Execution time - %v]", time.Since(startTime))
	return
}

func (c *Client) GetTempTablesNames(ctx context.Context) ([]string, error) {
	var res []string
	nsQuery := fmt.Sprintf(queries.GetTempTablesNames, TempTableInventoryTable)
	rows, err := c.conn.Query(ctx, nsQuery)
	if err != nil {
		log.Error(ctx, err, "could not execute query")
		return nil, err
	}

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			log.Error(ctx, err, "could not read data")
			return nil, err
		}

		res = append(res, table)
	}
	return res, nil
}

// CheckTempTableExists returns true if the table exists in the inventory
// returns error if some problem with executing query
func (c *Client) CheckTempTableExists(ctx context.Context, tableName string) (bool, error) {

	var exists bool
	err := c.conn.QueryRow(ctx, queries.CheckTempTableExists, tableName).Scan(&exists)
	if err != nil {
		log.Error(ctx, err, "could not check table existence")
		return false, err
	}
	return exists, nil
}

func (c *Client) GetS3FilesByStartTimeBetween(ctx context.Context, fromTs time.Time, toTs time.Time) (map[string]*inventory.S3FileInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetS3FilesByStartTimeBetween [from: %v, to: %v] ", fromTs, toTs)

	s3Files := make(map[string]*inventory.S3FileInfo)
	query := fmt.Sprintf(queries.GetInventoryByStartTimeBetween, S3FilesTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var file inventory.S3FileInfo
		var fileSize sql.NullInt64
		var servicesStr *string
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &file.StartTime, &file.EndTime, &file.Type, &file.DumpType, &file.Namespace,
			&file.DurationRange, &file.FileName, &file.Status, &servicesStr, &file.CreatedTime, &file.ApiVersion,
			&file.RowsCount, &fileSize, &file.RemoteStoragePath, &file.LocalFilePath); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
			return nil, err
		}

		file.Uuid = common.ToUuid(uuidBytes)

		if fileSize.Valid {
			file.FileSize = fileSize.Int64
		}

		if servicesStr != nil {
			set, err := jsonToMap[[]string](*servicesStr)
			if err != nil {
				log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
				return nil, err
			}
			file.Services = common.Ref(inventory.NewServices())
			file.Services.AddList(set)
		}

		s3Files[file.RemoteStoragePath] = &file
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetS3FilesByStartTimeBetween is finished. number of files: %d. [Execution time - %v]", len(s3Files), time.Since(startTime))

	return s3Files, nil
}

func (c *Client) GetCallsS3FilesByDurationRangeAndStartTimeBetween(ctx context.Context, durationRange model.DurationRange, fromTs time.Time, toTs time.Time) (map[string]*inventory.S3FileInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetCallsS3FilesByDurationRangeAndStartTimeBetween for duration range %s [from: %v, to: %v] ", durationRange.Title, fromTs, toTs)

	s3Files := make(map[string]*inventory.S3FileInfo)
	query := fmt.Sprintf(queries.GetS3FilesByDurationRangeAndStartTimeBetween, S3FilesTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, model.FileCalls, model.DurationAsInt(&durationRange), fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var file inventory.S3FileInfo
		var fileSize sql.NullInt64
		var servicesStr *string
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &file.StartTime, &file.EndTime, &file.Type, &file.DumpType, &file.Namespace,
			&file.DurationRange, &file.FileName, &file.Status, &servicesStr, &file.CreatedTime, &file.ApiVersion,
			&file.RowsCount, &fileSize, &file.RemoteStoragePath, &file.LocalFilePath); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
			return nil, err
		}

		file.Uuid = common.ToUuid(uuidBytes)

		if fileSize.Valid {
			file.FileSize = fileSize.Int64
		}

		if servicesStr != nil {
			set, err := jsonToMap[[]string](*servicesStr)
			if err != nil {
				log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
				return nil, err
			}
			file.Services = common.Ref(inventory.NewServices())
			file.Services.AddList(set)
		}

		s3Files[file.RemoteStoragePath] = &file
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetCallsS3FilesByDurationRangeAndStartTimeBetween is finished. number of files: %d. [Execution time - %v]", len(s3Files), time.Since(startTime))

	return s3Files, nil
}

func (c *Client) GetDumpsS3FilesByTypeAndStartTimeBetween(ctx context.Context, dumpType model.DumpType, fromTs time.Time, toTs time.Time) (map[string]*inventory.S3FileInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetDumpsS3FilesByTypeAndStartTimeBetween for dump type %s [from: %v, to: %v] ", dumpType, fromTs, toTs)

	s3Files := make(map[string]*inventory.S3FileInfo)
	query := fmt.Sprintf(queries.GetS3FilesByDumpTypeAndStartTimeBetween, S3FilesTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, model.FileDumps, dumpType, fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var file inventory.S3FileInfo
		var fileSize sql.NullInt64
		var servicesStr *string
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &file.StartTime, &file.EndTime, &file.Type, &file.DumpType, &file.Namespace,
			&file.DurationRange, &file.FileName, &file.Status, &servicesStr, &file.CreatedTime, &file.ApiVersion,
			&file.RowsCount, &fileSize, &file.RemoteStoragePath, &file.LocalFilePath); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
			return nil, err
		}

		file.Uuid = common.ToUuid(uuidBytes)

		if fileSize.Valid {
			file.FileSize = fileSize.Int64
		}

		if servicesStr != nil {
			set, err := jsonToMap[[]string](*servicesStr)
			if err != nil {
				log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
				return nil, err
			}
			file.Services = common.Ref(inventory.NewServices())
			file.Services.AddList(set)
		}

		s3Files[file.RemoteStoragePath] = &file
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetDumpsS3FilesByTypeAndStartTimeBetween is finished. number of files: %d. [Execution time - %v]", len(s3Files), time.Since(startTime))

	return s3Files, nil
}

func (c *Client) GetHeapsS3FilesByStartTimeBetween(ctx context.Context, fromTs time.Time, toTs time.Time) (map[string]*inventory.S3FileInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "Start execution GetHeapsS3FilesByStartTimeBetween [from: %v, to: %v] ", fromTs, toTs)

	s3Files := make(map[string]*inventory.S3FileInfo)
	query := fmt.Sprintf(queries.GetS3FilesByTypeAndStartTimeBetween, S3FilesTable)

	if log.IsDebugEnabled(ctx) {
		log.Debug(ctx, "execute query: %v ", query)
	}

	rows, err := c.conn.Query(ctx, query, model.FileHeap, fromTs, toTs)
	if err != nil {
		log.Error(ctx, err, "could not execute query %v", query)
		return nil, err
	}

	for rows.Next() {
		var file inventory.S3FileInfo
		var fileSize sql.NullInt64
		var servicesStr *string
		var uuidBytes [16]byte

		if err := rows.Scan(&uuidBytes, &file.StartTime, &file.EndTime, &file.Type, &file.DumpType, &file.Namespace,
			&file.DurationRange, &file.FileName, &file.Status, &servicesStr, &file.CreatedTime, &file.ApiVersion,
			&file.RowsCount, &fileSize, &file.RemoteStoragePath, &file.LocalFilePath); err != nil {
			log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
			return nil, err
		}

		file.Uuid = common.ToUuid(uuidBytes)

		if fileSize.Valid {
			file.FileSize = fileSize.Int64
		}

		if servicesStr != nil {
			set, err := jsonToMap[[]string](*servicesStr)
			if err != nil {
				log.Error(ctx, err, "problem during scan record from %s table", S3FilesTable)
				return nil, err
			}
			file.Services = common.Ref(inventory.NewServices())
			file.Services.AddList(set)
		}

		s3Files[file.RemoteStoragePath] = &file
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Debug(ctx, "GetHeapsS3FilesByStartTimeBetween is finished. number of files: %d. [Execution time - %v]", len(s3Files), time.Since(startTime))

	return s3Files, nil
}
