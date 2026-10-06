package sqlite

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	client "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

var (
	//go:embed resources/schema/*.sql
	schemaFS embed.FS
)

const (
	podTable       = "dump_pods"
	timelineTable  = "timeline"
	heapDumpsTable = "heap_dumps"
	Granularity    = time.Hour
)

// Client implements the database client for SQLite
type Client struct {
	db               *gorm.DB
	schemas          *template.Template
	dumpTableName    string
	partitionSchema  string
	usedParams       client.DBParams
	partitionsMu     *sync.RWMutex
	existingPartitions map[string]bool
}

func GranularTs(timestamp time.Time) int64 {
	return timestamp.UTC().Truncate(Granularity).Unix()
}

func (db *Client) prepareSchemaQuery(name string, args map[string]any) string {
	query := new(bytes.Buffer)
	if err := db.schemas.ExecuteTemplate(query, name, args); err != nil {
		return ""
	}
	return query.String()
}

func (db *Client) CloseConnection(ctx context.Context) error {
	sqlDB, err := db.db.DB()
	if err != nil {
		log.Error(ctx, err, "error getting connection")
		return err
	}
	if err := sqlDB.Close(); err != nil {
		log.Error(ctx, err, "error closing connection")
		return err
	}
	return nil
}

func (db *Client) HasTable(ctx context.Context, tableName string) bool {
	return db.db.Migrator().HasTable(tableName)
}

func (db *Client) DumpTable(ts time.Time) string {
	return fmt.Sprintf("%s_%d", db.dumpTableName, GranularTs(ts))
}

func (db *Client) GetParams() client.DBParams {
	return db.usedParams
}

// ensurePartitionExists creates a partition table if it doesn't exist
func (db *Client) ensurePartitionExists(ctx context.Context, ts time.Time) error {
	tableName := db.DumpTable(ts)

	db.partitionsMu.RLock()
	exists := db.existingPartitions[tableName]
	db.partitionsMu.RUnlock()

	if exists {
		return nil
	}

	db.partitionsMu.Lock()
	defer db.partitionsMu.Unlock()

	// Double check after acquiring write lock
	if db.existingPartitions[tableName] {
		return nil
	}

	// Check if table exists
	if db.HasTable(ctx, tableName) {
		db.existingPartitions[tableName] = true
		return nil
	}

	// Create partition table
	query := db.prepareSchemaQuery(db.partitionSchema, map[string]any{
		"TimeStamp": GranularTs(ts),
	})
	if query == "" {
		return fmt.Errorf("failed to prepare partition schema for %v", ts)
	}

	if err := db.db.Exec(query).Error; err != nil {
		return fmt.Errorf("failed to create partition table %s: %w", tableName, err)
	}

	// Record partition in metadata
	err := db.db.Exec(`INSERT OR IGNORE INTO dump_objects_partitions (partition_name, hour_epoch) VALUES (?, ?)`,
		tableName, GranularTs(ts)).Error
	if err != nil {
		log.Error(ctx, err, "Warning: failed to record partition %s in metadata", tableName)
	}

	db.existingPartitions[tableName] = true
	log.Info(ctx, "Created partition table: %s", tableName)
	return nil
}

// NewClient creates a new SQLite database client
func NewClient(ctx context.Context, params client.DBParams) (client.DumpDbClient, error) {
	if params.DBPath == "" {
		return nil, fmt.Errorf("SQLite database path is required")
	}

	// Configure SQLite connection
	// Use file path or :memory: for in-memory database
	db, err := gorm.Open(sqlite.Open(params.DBPath), &gorm.Config{
		CreateBatchSize: 200,
	})

	if err != nil {
		log.Error(ctx, err, "error opening SQLite database at %s", params.DBPath)
		return nil, err
	}

	// Set SQLite pragmas for better performance and concurrency
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying SQL DB: %w", err)
	}

	// Performance and concurrency settings
	_, err = sqlDB.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Error(ctx, err, "warning: failed to set WAL mode")
	}
	_, err = sqlDB.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		log.Error(ctx, err, "warning: failed to set synchronous mode")
	}
	_, err = sqlDB.Exec("PRAGMA busy_timeout=5000")
	if err != nil {
		log.Error(ctx, err, "warning: failed to set busy timeout")
	}
	_, err = sqlDB.Exec("PRAGMA cache_size=-64000") // 64MB cache
	if err != nil {
		log.Error(ctx, err, "warning: failed to set cache size")
	}

	// Parse schema templates
	schemas, err := template.ParseFS(schemaFS, "resources/schema/*.sql")
	if err != nil {
		log.Error(ctx, err, "failed to parse db schema from files")
		return nil, err
	}
	for _, tmpl := range schemas.Templates() {
		tmpl.Option("missingkey=error")
	}

	sqliteClient := &dumpDbClientImpl{
		Client: Client{
			db:                 db,
			schemas:            schemas,
			dumpTableName:      "dump_objects",
			partitionSchema:    "dump_objects_partition.sql",
			usedParams:         params,
			partitionsMu:       &sync.RWMutex{},
			existingPartitions: make(map[string]bool),
		},
	}

	// Initialize schema
	if err := sqliteClient.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	log.Info(ctx, "SQLite database client initialized successfully at %s", params.DBPath)
	return sqliteClient, nil
}

// initSchema initializes the database schema
func (db *dumpDbClientImpl) initSchema(ctx context.Context) error {
	schemaContent, err := schemaFS.ReadFile("resources/schema/schema.sql")
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	// Strip comment-only lines to avoid issues, then split into individual statements
	var statements []string
	for _, stmt := range splitSQLStatements(string(schemaContent)) {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	for _, stmt := range statements {
		if err := db.db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("failed to execute schema statement [%s]: %w", stmt[:min(len(stmt), 60)], err)
		}
	}

	log.Info(ctx, "SQLite schema initialized successfully")
	return nil
}

// splitSQLStatements splits SQL text into individual statements, handling
// comments and multi-line statements correctly.
func splitSQLStatements(sql string) []string {
	// First strip all comment lines (lines where first non-whitespace is --)
	var cleanLines []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		// Strip inline comments (-- after SQL)
		if idx := strings.Index(line, "--"); idx > 0 {
			line = line[:idx]
		}
		cleanLines = append(cleanLines, line)
	}
	cleaned := strings.Join(cleanLines, "\n")

	// Split by semicolons
	var statements []string
	for _, part := range strings.Split(cleaned, ";") {
		stmt := strings.TrimSpace(part)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

// dumpDbClientImpl implements DumpDbClient for SQLite
type dumpDbClientImpl struct {
	Client
}

func (db *dumpDbClientImpl) Transaction(ctx context.Context, fn func(tx client.DumpDbClient) error) error {
	startTime := time.Now()
	log.Debug(ctx, "[Transaction] started")

	err := db.db.Transaction(func(tx *gorm.DB) error {
		txClient := &dumpDbClientImpl{
			Client: Client{
				db:                 tx,
				schemas:            db.schemas,
				dumpTableName:      db.dumpTableName,
				partitionSchema:    db.partitionSchema,
				usedParams:         db.usedParams,
				partitionsMu:       db.partitionsMu,
				existingPartitions: db.existingPartitions,
			},
		}
		return fn(txClient)
	})

	duration := time.Since(startTime)
	log.Debug(ctx, "[Transaction] finished. Done in %v", duration)
	return err
}
