package db

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"
	"time"

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

type DbClient interface {
	// Common functions
	HasTable(ctx context.Context, tableName string) bool
	CloseConnection(ctx context.Context) error
	DumpTable(ts time.Time) string
	GetParams() DBParams
	// Pods
	CreatePodIfNotExist(ctx context.Context, namespace string, serviceName string, podName string, restartTime time.Time) (*model.Pod, bool, error)
	GetPodsCount(ctx context.Context) (int64, error)
	GetPodById(ctx context.Context, id uuid.UUID) (*model.Pod, error)
	FindPod(ctx context.Context, namespace string, serviceName string, podName string) (*model.Pod, error)
	SearchPods(ctx context.Context, podFilter model.PodFilter) ([]model.Pod, error)
	UpdatePodLastActive(ctx context.Context, namespace string, serviceName string, podName string, restartTime time.Time, lastActive time.Time) (*model.Pod, error)
	RemoveOldPods(ctx context.Context, activeBefore time.Time) ([]model.Pod, error)
	// Timelines
	CreateTimelineIfNotExist(ctx context.Context, t time.Time) (*model.Timeline, bool, error)
	FindTimeline(ctx context.Context, t time.Time) (*model.Timeline, error)
	SearchTimelines(ctx context.Context, dateFrom time.Time, dateTo time.Time) ([]model.Timeline, error)
	UpdateTimelineStatus(ctx context.Context, t time.Time, status model.TimelineStatus) (*model.Timeline, error)
	RemoveTimeline(ctx context.Context, t time.Time) (*model.Timeline, error)
	// Heap dumps
	CreateHeapDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.HeapDump, bool, error)
	InsertHeapDumps(ctx context.Context, dumps []model.DumpInfo) ([]model.HeapDump, error)
	GetHeapDumpsCount(ctx context.Context) (int64, error)
	FindHeapDump(ctx context.Context, handle string) (*model.HeapDump, error)
	SearchHeapDumps(ctx context.Context, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.HeapDump, error)
	RemoveOldHeapDumps(ctx context.Context, createdBefore time.Time) ([]model.HeapDump, error)
	TrimHeapDumps(ctx context.Context, limitPerPod int) ([]model.HeapDump, error)
	// td/top dumps
	CreateTdTopDumpIfNotExist(ctx context.Context, dump model.DumpInfo) (*model.DumpObject, bool, error)
	InsertTdTopDumps(ctx context.Context, tHour time.Time, dumps []model.DumpInfo) ([]model.DumpObject, error)
}

type DumpDbClient interface {
	DbClient
	Transaction(ctx context.Context, fn func(tx DumpDbClient) error) error
	// Td/top dumps
	FindTdTopDump(ctx context.Context, podId uuid.UUID, creationTime time.Time, dumpType model.DumpType) (*model.DumpObject, error)
	GetTdTopDumpsCount(ctx context.Context, tHour time.Time, dateFrom time.Time, dateTo time.Time) (int64, error)
	SearchTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time, dumpType model.DumpType) ([]model.DumpObject, error)
	CalculateSummaryTdTopDumps(ctx context.Context, tHour time.Time, podIds []uuid.UUID, dateFrom time.Time, dateTo time.Time) ([]model.DumpSummary, error)
	RemoveOldTdTopDumps(ctx context.Context, tHour time.Time, createdBefore time.Time) ([]model.DumpObject, error)
	StoreDumpsTransactionally(ctx context.Context, heapDumpsArray []model.DumpInfo, tdTopDumpsArray []model.DumpInfo, tMinute time.Time) (model.StoreDumpResult, error)
}

type Client struct {
	db              *gorm.DB
	schemas         *template.Template
	dumpTableName   string
	dumpTableSchema string
	usedParams      DBParams
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

func (db *Client) GetParams() DBParams {
	return db.usedParams
}

func NewDumpDbClient(ctx context.Context, params DBParams) (DumpDbClient, error) {
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
