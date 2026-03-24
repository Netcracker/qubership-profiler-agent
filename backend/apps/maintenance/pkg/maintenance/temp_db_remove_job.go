package maintenance

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
)

// TempTablesRemoveJob is a job to remove outdated temp tables
type TempTablesRemoveJob struct {
	*MaintenanceJob
	toTs time.Time
}

// NewTempTablesRemoveJob creates the new NewTempTablesRemoveJob
func NewTempTablesRemoveJob(ctx context.Context, mJob *MaintenanceJob, ts time.Time) (*TempTablesRemoveJob, error) {
	toTs := ts.Add(time.Duration(-mJob.JobConfig.TempTableRemoval) * time.Hour)
	log.Info(ctx, "Create new TempTablesRemoveJob. Compaction time: %v", ts)

	return &TempTablesRemoveJob{
		MaintenanceJob: mJob,
		toTs:           toTs,
	}, nil
}

// Execute is the main method for job
func (trj *TempTablesRemoveJob) Execute(ctx context.Context) error {
	startTime := time.Now()

	tablesToRemove, err := trj.getTablesToRemove(ctx)
	if err != nil {
		log.Error(ctx, err, "Error calculating tables that should be removed")
		return err
	}

	// Update the status for tables
	var tablesNamesToRemove = make([]string, 0, len(tablesToRemove))
	for _, table := range tablesToRemove {
		table.Status = model.TableStatusToDelete
		if err := trj.Postgres.UpdateTempTableInventory(ctx, *table); err != nil {
			log.Error(ctx, err, "error updating the status for temp table %s", table)
		} else {
			tablesNamesToRemove = append(tablesNamesToRemove, table.TableName)
		}
	}

	// Drop tables
	if err := trj.Postgres.DropTables(ctx, tablesNamesToRemove...); err != nil {
		log.Error(ctx, err, "error removing temp tables %v", tablesNamesToRemove)
		return err
	}

	// Remove information about removed table
	successfulTables := 0
	for _, tableName := range tablesNamesToRemove {
		table := tablesToRemove[tableName]
		log.Debug(ctx, "Start removing table row %s with start time %v", tableName, table.StartTime)
		if err := trj.Postgres.RemoveTempTableInventory(ctx, table.Uuid); err != nil {
			log.Error(ctx, err, "error removing temp table inventory row %s", tableName)
		} else {
			successfulTables++
		}
	}

	log.Info(ctx, "TempTablesRemoveJob for %v is finished. Removed %d tables. [Execution time - %v]", trj.toTs, successfulTables, time.Since(startTime))
	return nil
}

// getTablesToRemove create the list of tables, that should be removed
func (trj *TempTablesRemoveJob) getTablesToRemove(ctx context.Context) (map[string]*inventory.TempTableInfo, error) {
	// Get already existed tables from specified time range
	existTempTables, err := trj.Postgres.GetTempTableByEndTimeBetween(ctx, time.Time{}, trj.toTs)
	if err != nil {
		return nil, err
	}

	var tablesToRemove = make(map[string]*inventory.TempTableInfo)
	// Check the status of exist tables and calculate the list of unexist ones
	for _, tempTable := range existTempTables {
		if trj.checkTablesWithStatus(ctx, tempTable) {
			tablesToRemove[tempTable.TableName] = tempTable
		}
	}

	// Return the result
	return tablesToRemove, nil
}

// checkTablesWithStatus checks if specified table exists in the db and warns, if it exists with not persisted status (possible error)
func (trj *TempTablesRemoveJob) checkTablesWithStatus(ctx context.Context, tempTable *inventory.TempTableInfo) bool {
	// Warn, if table has not persisted status
	// TODO: try to remove tables with to_delete status?
	if tempTable.Status != model.TableStatusPersisted {
		log.Warning(ctx, "Found temp table table with unexpected status: table name = %s, table status = %s", tempTable.TableName, tempTable.Status)
		return false
	}
	return true
}
