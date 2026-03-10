package postgres

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/plugin/prometheus"
)

var (
	//go:embed resources/schema/*.sql
	schemaFS embed.FS
)

const (
	schemaFile     = "schema.sql"
	podTable       = "dump_pods"
	timelineTable  = "timeline"
	heapDumpsTable = "heap_dumps"

	Granularity = time.Hour
)

// Client implements the database client for PostgreSQL
type Client struct {
	db              *gorm.DB
	schemas         *template.Template
	dumpTableName   string
	dumpTableSchema string
	usedParams      client.DBParams
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

// NewClient creates a new PostgreSQL database client
func NewClient(ctx context.Context, params client.DBParams) (client.DumpDbClient, error) {
	var dbClient dumpDbClientImpl
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
			params.DBHost, params.DBPort,
			params.DBUser, params.DBPassword,
			params.DBName),
		PreferSimpleProtocol: true,
	}),
		&gorm.Config{
			CreateBatchSize: 200,
		})

	if err != nil {
		log.Error(ctx, err, "error opening db")
		return nil, err
	}

	if params.EnableMetrics {
		if err := db.Use(prometheus.New(prometheus.Config{
			DBName: params.DBName,
			MetricsCollector: []prometheus.MetricsCollector{
				&prometheus.Postgres{
					Interval: 30,
				},
			},
		})); err != nil {
			log.Error(ctx, err, "error enabling prometheus metrics")
		}
	}

	schemas, err := template.ParseFS(schemaFS, "resources/schema/*.sql")
	if err != nil {
		log.Error(ctx, err, "failed to parse db schema from files")
		return nil, err
	}
	for _, tmpl := range schemas.Templates() {
		tmpl.Option("missingkey=error")
	}
	dbClient = dumpDbClientImpl{
		Client{
			db:              db,
			schemas:         schemas,
			dumpTableName:   "dump_objects",
			dumpTableSchema: "dump_objects_schema.sql",
			usedParams:      params,
		},
	}

	return &dbClient, nil
}

// dumpDbClientImpl implements DumpDbClient for PostgreSQL
type dumpDbClientImpl struct {
	Client
}

func (db *dumpDbClientImpl) Transaction(ctx context.Context, fn func(tx client.DumpDbClient) error) error {
	startTime := time.Now()
	log.Debug(ctx, "[Transaction] started")

	err := db.db.Transaction(func(tx *gorm.DB) error {
		txClient := &dumpDbClientImpl{
			Client: Client{
				db:            tx,
				dumpTableName: db.dumpTableName,
			},
		}
		return fn(txClient)
	})

	duration := time.Since(startTime)
	// metrics.AddPgOperationMetricValue(metrics.NoEntity, metrics.PgOperationTransaction, duration, 0, err != nil)

	log.Debug(ctx, "[Transaction] finished. Done in %v", duration)
	return err
}
