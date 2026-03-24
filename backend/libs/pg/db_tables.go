package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg/queries"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/golang-migrate/migrate/v4"
	pgxmigr "github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

func (c *Client) InitSchema(ctx context.Context) error {
	startTime := time.Now()
	err := c.MigrateSchema(ctx)
	if err == nil {
		log.Info(ctx, "Updated database schema in %v", time.Since(startTime))
	}
	return err
}

func (c *Client) MigrateSchema(ctx context.Context) error {
	migr, err := c.initializeMigration(ctx)
	if err != nil {
		return err
	}
	defer c.releaseMigration(ctx, migr)
	if err = migr.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	ver, dirty, _ := migr.Version()
	log.Info(ctx, "schema migration has been finished: version=%v, dirty=%v", ver, dirty)
	return nil
}

func (c *Client) RollbackSchema(ctx context.Context) error {
	migr, err := c.initializeMigration(ctx)
	if err != nil {
		return err
	}
	defer c.releaseMigration(ctx, migr)
	if err = migr.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	ver, dirty, _ := migr.Version()
	log.Info(ctx, "schema migration has been finished: version=%v, dirty=%v", ver, dirty)
	return nil
}

func (c *Client) initializeMigration(ctx context.Context) (*migrate.Migrate, error) {
	srcDriver, err := iofs.New(migrationFS, "resources/schema/migration")
	if err != nil {
		log.Error(ctx, err, "error initializing migration source driver")
		return nil, err
	}

	db, err := sql.Open(pgxDriverName, c.conn.Config().ConnString())
	if err != nil {
		return nil, err
	}
	dbDriver, err := pgxmigr.WithInstance(db, &pgxmigr.Config{})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	migr, err := migrate.NewWithInstance(iofsDriverName, srcDriver, pgxDriverName, dbDriver)
	if err != nil {
		// dbDriver.Close() invokes db.Close() as well
		_ = dbDriver.Close()
		return nil, err
	}
	migr.Log = migrLogFunc{ctx}
	return migr, nil
}

func (c *Client) releaseMigration(ctx context.Context, migr *migrate.Migrate) {
	srcErr, dbErr := migr.Close()
	if srcErr != nil {
		log.Warning(ctx, "can't close source driver: %v", srcErr)
	}
	if dbErr != nil {
		log.Warning(ctx, "can't close DB driver: %v", dbErr)
	}
}

type migrLogFunc struct {
	ctx context.Context
}

func (f migrLogFunc) Printf(msg string, args ...interface{}) {
	log.Info(f.ctx, msg, args)
}

func (f migrLogFunc) Verbose() bool {
	return log.IsDebugEnabled(f.ctx)
}

// CreateTempTables uses only for integration tests
func (c *Client) CreateTempTables(ctx context.Context, t time.Time) ([]*inventory.TempTableInfo, error) {
	startTime := time.Now()
	log.Debug(ctx, "[CreateTempTables] started for ts %v", t)

	var tempTables = make([]*inventory.TempTableInfo, 0, 3)

	callsTable, err := c.CreateCallsTempTable(ctx, t)
	if err != nil {
		return tempTables, err
	}
	tempTables = append(tempTables, callsTable)

	traceTable, err := c.CreateTracesTempTable(ctx, t)
	if err != nil {
		return tempTables, err
	}
	tempTables = append(tempTables, traceTable)

	dumpTable, err := c.CreateDumpsTempTable(ctx, t)
	if err != nil {
		return tempTables, err
	}

	if dumpTable != nil {
		tempTables = append(tempTables, dumpTable)
	}

	suspendTable, err := c.CreateSuspendTempTable(ctx, t)
	if err != nil {
		return tempTables, err
	}
	tempTables = append(tempTables, suspendTable)

	for _, p := range strings.Split(InvertedIndexParams, ",") {
		prefix, err := common.NormalizeParam(p)
		if err != nil {
			return tempTables, fmt.Errorf("invalid param format: %w", err)
		}
		ts := t.Truncate(InvertedIndexGranularity)
		invertedIndexTable, err := c.CreateInvertedIndexTable(ctx, ts, InvertedIndexTable(prefix, ts), InvertedIndexLifetime)
		if err != nil {
			return tempTables, err
		}
		if invertedIndexTable != nil {
			tempTables = append(tempTables, invertedIndexTable)
		}
	}

	log.Debug(ctx, "[CreateTempTables] done in %v", time.Since(startTime))
	return tempTables, nil
}

// CreateCallsTempTable registers and creates a temp table for call data at the given timestamp
func (c *Client) CreateCallsTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error) {
	schemaArgs := map[string]any{
		"TimeStamp": GranularTs(t, Granularity),
	}
	return c.createTempTable(ctx, CallsTable(t), model.TableCalls, t, TempTableLifetime, CallsTempSchema, schemaArgs)
}

// CreateTracesTempTable registers and creates a temp table for traces data at the given timestamp
func (c *Client) CreateTracesTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error) {
	schemaArgs := map[string]any{
		"TimeStamp": GranularTs(t, Granularity),
	}
	return c.createTempTable(ctx, TracesTable(t), model.TableTraces, t, TempTableLifetime, TracesTempSchema, schemaArgs)
}

// CreateSuspendTempTable registers and creates a temp table for suspend data at the given timestamp
func (c *Client) CreateSuspendTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error) {
	schemaArgs := map[string]any{
		"TimeStamp": GranularTs(t, Granularity),
	}
	return c.createTempTable(ctx, SuspendTable(t), model.TableSuspend, t, TempTableLifetime, SuspendTempSchema, schemaArgs)
}

// CreateDumpsTempTable registers and creates a temp table for dumps data at the given timestamp
func (c *Client) CreateDumpsTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error) {
	timeHour := t.Truncate(DumpsTableGranularity)
	tableName := DumpsTable(timeHour)
	from := timeHour.Format("2006-01-02 15:04:05")
	to := timeHour.Add(DumpsTableGranularity).Format("2006-01-02 15:04:05")

	// Check required because dumps is a partitioned table and PARTITION OF doesn't support IF NOT EXISTS
	exists, err := c.CheckTempTableExists(ctx, tableName)
	if err != nil {
		log.Error(ctx, err, "error checking table")
	} else if exists {
		return nil, nil
	}

	schemaArgs := map[string]any{
		"TimeStamp": GranularTs(timeHour, DumpsTableGranularity),
		"From":      fmt.Sprintf("('%s')", from),
		"To":        fmt.Sprintf("('%s')", to),
	}

	return c.createTempTable(ctx, tableName, model.TableDumps, timeHour, DumpsTableLifetime, DumpsTempSchema, schemaArgs)
}

// CreateInvertedIndexTable registers and creates a temp table for dumps data at the given timestamp
func (c *Client) CreateInvertedIndexTable(ctx context.Context, t time.Time, tableName string, ttl time.Duration) (*inventory.TempTableInfo, error) {

	// TODO: Prepare reason of using this check
	exists, err := c.CheckTempTableExists(ctx, tableName)
	if err != nil {
		log.Error(ctx, err, "error checking table")
	} else if exists {
		return nil, nil
	}

	schemaArgs := map[string]any{
		"TableName": tableName,
	}
	return c.createTempTable(ctx, tableName, model.TableInvertedIndex, t, ttl, InvertedIndexSchema, schemaArgs)
}

// createTempTable registers a temp table in the inventory (status=creating)
// and executes SQL to create the table in the database.
func (c *Client) createTempTable(
	ctx context.Context,
	tableName string,
	tableType model.TableType,
	t time.Time,
	ttl time.Duration,
	schema string,
	schemaArgs map[string]any,
) (*inventory.TempTableInfo, error) {
	startTime := time.Now()

	// Initialize and persist temp table metadata (status=creating)
	table, err := c.registerTempTable(ctx, tableName, tableType, t, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to register temp table: %w", err)
	}

	// Generate SQL to create the temp table
	query, err := c.PrepareSchemaQuery(schema, schemaArgs)
	if err != nil {
		return table, fmt.Errorf("failed to prepare schema query: %w", err)
	}

	// Run SQL to create the temp table in the database
	if _, err := c.conn.Exec(ctx, query); err != nil {
		return table, fmt.Errorf("failed to execute create table statement: %w", err)
	}

	log.Debug(ctx, "Created temp table %s (type=%s, ts=%s) in %v",
		tableName, tableType, table.StartTime.Format(time.RFC3339), time.Since(startTime))

	return table, nil
}

// registerTempTable creates a TempTableInfo object for the given time range and type,
// initializes it with default metadata (status=creating), and inserts it into the inventory table.
// Returns a pointer to the inserted inventory.TempTableInfo or an error if the DB insert fails.
func (c *Client) registerTempTable(ctx context.Context, tableName string, tableType model.TableType, ts time.Time, ttl time.Duration) (*inventory.TempTableInfo, error) {
	table := inventory.TempTableInfo{
		Uuid:           common.RandomUuid(),
		StartTime:      ts,
		EndTime:        ts.Add(ttl),
		Status:         model.TableStatusCreating,
		Type:           tableType,
		TableName:      tableName,
		CreatedTime:    time.Now(),
		RowsCount:      0,
		TableSize:      0,
		TableTotalSize: 0,
	}
	if err := c.InsertTempTableInventory(ctx, table); err != nil {
		return nil, err
	}
	return &table, nil
}

func (c *Client) DropTables(ctx context.Context, tables ...string) error {
	for _, t := range tables {
		query := fmt.Sprintf(queries.DropTables, t)
		_, err := c.conn.Exec(ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) TruncateCDTTables(ctx context.Context) error {
	return c.TruncateTables(ctx, TempTableInventoryTable, S3FilesTable, PodsTable, PodRestartsTable, DictionaryTable, ParamsTable)
}

func (c *Client) TruncateTables(ctx context.Context, tables ...string) error {
	for _, t := range tables {
		query := fmt.Sprintf(queries.TruncateTable, t)
		_, err := c.conn.Exec(ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) CloseConnection(ctx context.Context) {
	c.conn.Close()
	log.Info(ctx, "Closed a connection")
}
