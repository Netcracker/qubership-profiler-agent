package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
)

const (
	// CreateTablesCapacity defines the estimated number of tables created per 2-hour time range.
	// Calculated as follows:
	//   - Tables (calls, traces, suspend) for every 5-minute interval → 25 intervals * 3 = 75 tables
	//   - 3 dump tables for every hour → 3 tables
	//   - Inverted index tables: 2 parameters * 3 intervals = 6 tables
	// Total: 75 + 3 + 6 = 84 tables
	CreateTablesCapacity = 84
)

// tempTableToCreate represents a temporary table that needs to be created.
type tempTableToCreate struct {
	tableType model.TableType
	tableName string
	ts        time.Time
}

// TempTablesCreationJob is a job to create future temp tables
type TempTablesCreationJob struct {
	*MaintenanceJob
	fromTs time.Time
	toTs   time.Time
}

// NewTempTablesCreationJob constructs a TempTablesCreationJob for the specified time range.
// The range starts at ts and spans TempTableCreation hours, as defined in the job configuration.
func NewTempTablesCreationJob(ctx context.Context, mJob *MaintenanceJob, ts time.Time) (*TempTablesCreationJob, error) {
	fromTs := ts.Truncate(time.Hour)
	toTs := fromTs.Add(time.Duration(mJob.JobConfig.TempTableCreation) * time.Hour)
	log.Info(ctx, "Initializing TempTablesCreationJob: from %v to %v",
		fromTs.Format(time.RFC3339), toTs.Format(time.RFC3339))

	return &TempTablesCreationJob{
		MaintenanceJob: mJob,
		fromTs:         fromTs,
		toTs:           toTs,
	}, nil
}

// Execute runs the full temp table creation workflow:
// - Collects a list of temp tables that should be created for the time range
// - Creates missing temp tables and inserts them into the inventory with "creating" status
// - Updates status of successfully created tables to "ready"
//
// Logs the number of successfully created tables and total execution time.
func (tcj *TempTablesCreationJob) Execute(ctx context.Context) error {
	startTime := time.Now()

	// Step 1: Collect tables that need to be created
	tablesToCreate, err := tcj.getTablesToCreate(ctx)
	if err != nil {
		return fmt.Errorf("failed to determine temp tables to create: %w", err)
	}

	// Step 2: Create missing tables and insert them into inventory with the "creating" status
	var createdTables = make([]*inventory.TempTableInfo, 0, CreateTablesCapacity)
	for _, tableToCreate := range tablesToCreate {
		creationTime := time.Now()
		tempTableInfo, err := tcj.createTempTable(ctx, tableToCreate)
		if err == nil && tempTableInfo != nil {
			createdTables = append(createdTables, tempTableInfo)
			log.Info(ctx, "Created temp table %s (type=%s, ts=%s) in %v",
				tempTableInfo.TableName, tempTableInfo.Type, tempTableInfo.StartTime.Format(time.RFC3339), time.Since(creationTime))
		} else {
			log.Error(ctx, err, "Failed to create temp table %s (type=%s, ts=%s)",
				tableToCreate.tableName, tableToCreate.tableType, tableToCreate.ts.Format(time.RFC3339))
		}
	}

	// Step 3: Update inventory to mark created tables as "ready"
	successfulTables := 0
	for _, tempTable := range createdTables {
		tempTable.Status = model.TableStatusReady
		if err := tcj.Postgres.UpdateTempTableInventory(ctx, *tempTable); err != nil {
			log.Error(ctx, err, "Failed to update status for temp table %s (type=%s, ts=%s)",
				tempTable.TableName, tempTable.Type, tempTable.StartTime.Format(time.RFC3339))
		} else {
			successfulTables++
			// TODO: Is there a need for a log of the successful update and the time spent?
		}
	}

	// TODO: Possible metrics: created temp tables count per iteration
	log.Info(ctx, "Finished TempTablesCreationJob: from %v to %v, created %d tables in %v",
		tcj.fromTs.Format(time.RFC3339), tcj.toTs.Format(time.RFC3339), successfulTables, time.Since(startTime))
	return nil
}

// getTablesToCreate returns a list of tempTableToCreate that need to be created
// for the current job's time window. It compares the expected tables (based on configured
// granularity) against the existing inventory and collects the missing ones.
//
// The following table types are checked:
//   - dumps (per hour)
//   - inverted indexes (per param, per configured granularity)
//   - calls / traces / suspend (per 5-minute interval)
//
// Only tables that are missing in the inventory are included in the result.
func (tcj *TempTablesCreationJob) getTablesToCreate(ctx context.Context) ([]tempTableToCreate, error) {
	var tablesToCreate = make([]tempTableToCreate, 0, CreateTablesCapacity)

	// Fetch existing temporary tables from the inventory for the target time range
	existTempTables, err := tcj.Postgres.GetTempTableByStartTimeBetween(ctx, tcj.fromTs, tcj.toTs)
	if err != nil {
		return nil, err
	}

	// Iterate over the time range using hourly granularity to detect missing dump tables.
	// Example (range: 10:00–12:00):
	//   ts = 10:00:00
	//   ts = 11:00:00
	//   ts = 12:00:00
	for ts := tcj.fromTs; !ts.After(tcj.toTs); ts = ts.Add(pg.DumpsTableGranularity) {
		table := pg.DumpsTable(ts)
		tcj.appendIfMissing(ctx, table, &tablesToCreate, existTempTables, model.TableDumps, ts)
	}

	// Iterate over the time range using custom granularity for each prefix parameter.
	// Missing inverted index tables are added to the creation list.
	// Example (range: 10:00–12:00, granularity = 1 h):
	//   ts = 10:00:00
	//   ts = 11:00:00
	//   ts = 12:00:00
	for ts := tcj.fromTs; !ts.After(tcj.toTs); ts = ts.Add(tcj.InvertedIndexConfig.Granularity) {
		for _, param := range tcj.InvertedIndexConfig.Prefixes {
			table := pg.InvertedIndexTable(param, ts)
			tcj.appendIfMissing(ctx, table, &tablesToCreate, existTempTables, model.TableInvertedIndex, ts)
		}
	}

	// Iterate over the time range using 5-minute granularity to collect missing
	// calls, traces, and suspend tables.
	// Example (range: 10:00–12:00, granularity = 5 m):
	//   ts = 10:00:00
	//   ts = 10:05:00
	//   ...
	//   ts = 12:00:00
	for ts := tcj.fromTs; !ts.After(tcj.toTs); ts = ts.Add(pg.Granularity) {
		tcj.appendIfMissing(ctx, pg.CallsTable(ts), &tablesToCreate, existTempTables, model.TableCalls, ts)
		tcj.appendIfMissing(ctx, pg.TracesTable(ts), &tablesToCreate, existTempTables, model.TableTraces, ts)
		tcj.appendIfMissing(ctx, pg.SuspendTable(ts), &tablesToCreate, existTempTables, model.TableSuspend, ts)
	}

	// Return the result
	return tablesToCreate, nil
}

// appendIfMissing adds a tempTableToCreate to the list only if the table
// with the given name is not present in the inventory map (regardless of its status).
func (tcj *TempTablesCreationJob) appendIfMissing(
	ctx context.Context,
	tableName string,
	tablesToCreate *[]tempTableToCreate,
	existTables map[string]*inventory.TempTableInfo,
	tableType model.TableType,
	ts time.Time,
) {
	if !tcj.checkTablesWithStatus(ctx, existTables, tableName) {
		*tablesToCreate = append(*tablesToCreate, tempTableToCreate{
			tableType: tableType,
			tableName: tableName,
			ts:        ts,
		})
	}
}

// checkTablesWithStatus checks whether a temporary table with the given name
// already exists in the provided inventory map.
// Returns true if the table exists (regardless of its status).
// Logs a warning if the table exists but is not in "ready" status.
func (tcj *TempTablesCreationJob) checkTablesWithStatus(ctx context.Context, existTempTables map[string]*inventory.TempTableInfo, tableName string) bool {
	if existTable, ok := existTempTables[tableName]; ok {
		if existTable.Status != model.TableStatusReady {
			// TODO: decide whether to recreate temp tables stuck in "creating" status
			log.Warning(ctx, "Temp table %s (type=%s, ts=%s) has unexpected status: expected=ready actual=%s",
				tableName, existTable.Type, existTable.StartTime.Format(time.RFC3339), existTable.Status)
		}
		log.Debug(ctx, "Temp table %s (type=%s, ts=%s) already exists, skipping",
			tableName, existTable.Type, existTable.StartTime.Format(time.RFC3339))
		return true
	}
	return false
}

// createTempTable creates the temp table for a specified timestamp and table type
func (tcj *TempTablesCreationJob) createTempTable(ctx context.Context, tempTableToCreate tempTableToCreate) (*inventory.TempTableInfo, error) {
	switch tempTableToCreate.tableType {
	case model.TableCalls:
		return tcj.Postgres.CreateCallsTempTable(ctx, tempTableToCreate.ts)
	case model.TableTraces:
		return tcj.Postgres.CreateTracesTempTable(ctx, tempTableToCreate.ts)
	case model.TableDumps:
		return tcj.Postgres.CreateDumpsTempTable(ctx, tempTableToCreate.ts)
	case model.TableSuspend:
		return tcj.Postgres.CreateSuspendTempTable(ctx, tempTableToCreate.ts)
	case model.TableInvertedIndex:
		return tcj.Postgres.CreateInvertedIndexTable(ctx, tempTableToCreate.ts, tempTableToCreate.tableName, tcj.InvertedIndexConfig.Lifetime)
	default:
		return nil, fmt.Errorf("unsupported temp table type %s", tempTableToCreate.tableType)
	}
}
