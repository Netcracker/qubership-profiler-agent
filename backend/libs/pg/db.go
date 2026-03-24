package pg

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"net/url"
	"text/template"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/IBM/pgxpoolprometheus"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	TempTableInventoryTable = "temp_table_inventory"
	PodsTable               = "pods"
	S3FilesTable            = "s3_files"
	DictionaryTable         = "dictionary"
	PodRestartsTable        = "pod_restarts"
	ParamsTable             = "params"

	Granularity       = 5 * time.Minute // granularity for temp tables (calls, traces, suspend)
	TempTableLifetime = 5 * time.Minute // lifetime for temp tables (calls, traces, suspend)

	DumpsTableGranularity = 1 * time.Hour   // granularity for dumps temp tables
	DumpsTableLifetime    = 168 * time.Hour // lifetime for dumps temp tables (7 days)

	InvertedIndexGranularity = 1 * time.Hour   // granularity for inverted index temp tables
	InvertedIndexLifetime    = 336 * time.Hour // lifetime for inverted index temp tables (2 weeks)
	InvertedIndexParams      = "request.id,trace.id"
)

var (
	//go:embed "resources/schema/migration"
	migrationFS embed.FS
	//go:embed resources/schema/*.gosql
	schemaFS embed.FS
	//go:embed resources/queries/data/*.gosql
	//go:embed resources/queries/inventory/*.gosql
	//go:embed resources/queries/meta/*.gosql
	queriesFS embed.FS
)

const (
	CallsTempSchema     = "calls_tables_template.gosql"
	TracesTempSchema    = "traces_tables_template.gosql"
	DumpsTempSchema     = "dumps_tables_template.gosql"
	SuspendTempSchema   = "suspend_tables_template.gosql"
	InvertedIndexSchema = "inverted_index_template.gosql"

	pgxDriverName  = "pgx"
	iofsDriverName = "iofs"
)

type DbClient interface {

	// Table operations

	InitSchema(ctx context.Context) error
	CreateTempTables(ctx context.Context, t time.Time) ([]*inventory.TempTableInfo, error)
	CreateCallsTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error)
	CreateTracesTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error)
	CreateDumpsTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error)
	CreateSuspendTempTable(ctx context.Context, t time.Time) (*inventory.TempTableInfo, error)
	CreateInvertedIndexTable(ctx context.Context, t time.Time, tableName string, ttl time.Duration) (*inventory.TempTableInfo, error)
	DropTables(ctx context.Context, tables ...string) error
	TruncateCDTTables(ctx context.Context) error
	TruncateTables(ctx context.Context, tables ...string) error

	// Create operations

	InsertS3File(ctx context.Context, file inventory.S3FileInfo) error
	InsertInvertedIndex(ctx context.Context, tableTime time.Time, indexes *index.Map) error
	InsertTempTableInventory(ctx context.Context, table inventory.TempTableInfo) error
	InsertPod(ctx context.Context, pod model.PodInfo) error
	InsertPodRestart(ctx context.Context, pod model.PodRestart) error
	InsertParam(ctx context.Context, param model.Param) error
	InsertDictionary(ctx context.Context, dict model.Dictionary) error
	InsertCall(ctx context.Context, call model.Call) error
	InsertTrace(ctx context.Context, t time.Time, trace model.Trace) error
	InsertDump(ctx context.Context, dump model.Dump) error

	// Read operations

	GetTempTableByStatusAndStartTimeBetween(ctx context.Context, status model.TableStatus, from time.Time, to time.Time) (map[string]*inventory.TempTableInfo, error)
	GetTempTableByStartTimeBetween(ctx context.Context, from time.Time, to time.Time) (map[string]*inventory.TempTableInfo, error)
	GetTempTableByEndTimeBetween(ctx context.Context, from time.Time, to time.Time) (map[string]*inventory.TempTableInfo, error)
	GetUniqueNamespaces(ctx context.Context) ([]string, error)
	GetUniquePodsForNamespaceActiveAfter(ctx context.Context, namespace string, activeAfter time.Time) ([]*model.PodInfo, error)
	GetUniquePodsForNamespaceActiveBefore(ctx context.Context, namespace string, activeBefore time.Time) ([]*model.PodInfo, error)
	GetPodRestarts(ctx context.Context, namespace string, service string, podName string) ([]*model.PodRestart, error)
	GetCallsTimeBetween(ctx context.Context, namespace string, ts time.Time) ([]*model.Call, error)
	GetCallsWithTraceTimeBetween(ctx context.Context, namespace string, pod *model.PodInfo, callTbName, traceTbName string, upperBound, lowerBound time.Time) ([]*model.CallWithTraces, error)
	GetTagByPosition(ctx context.Context, position int) (string, error)
	GetTableMetadata(ctx context.Context, tbName string) (rowsCount int, size int64, totalSize int64, err error)
	GetDumpsTimeBetween(ctx context.Context, pod *model.PodInfo, dumpsTbName string, upperBound, lowerBound time.Time) ([]*model.Dump, error)
	GetTempTablesNames(ctx context.Context) ([]string, error)
	CheckTempTableExists(ctx context.Context, tableName string) (bool, error)
	GetS3FilesByStartTimeBetween(ctx context.Context, from time.Time, to time.Time) (map[string]*inventory.S3FileInfo, error)
	GetCallsS3FilesByDurationRangeAndStartTimeBetween(ctx context.Context, durationRange model.DurationRange, from time.Time, to time.Time) (map[string]*inventory.S3FileInfo, error)
	GetDumpsS3FilesByTypeAndStartTimeBetween(ctx context.Context, dumpType model.DumpType, from time.Time, to time.Time) (map[string]*inventory.S3FileInfo, error)
	GetHeapsS3FilesByStartTimeBetween(ctx context.Context, from time.Time, to time.Time) (map[string]*inventory.S3FileInfo, error)

	// Update operations

	UpdateTempTableInventory(ctx context.Context, info inventory.TempTableInfo) error
	UpdateS3File(ctx context.Context, file inventory.S3FileInfo) error

	// Remove operations

	RemoveTempTableInventory(ctx context.Context, uuid common.Uuid) error
	RemoveS3File(ctx context.Context, uuid common.Uuid) error
	RemovePod(ctx context.Context, podId string) error
	RemovePodRestart(ctx context.Context, podId string) error
}

type Client struct {
	conn    *pgxpool.Pool
	schemas *template.Template
	queries *template.Template
}

func NewClient(ctx context.Context, postgresParams Params) (*Client, error) {
	schemas, err := template.ParseFS(schemaFS, "resources/schema/*.gosql")
	if err != nil {
		log.Error(ctx, err, "failed to parse db schema from files")
		return nil, err
	}
	for _, tmpl := range schemas.Templates() {
		tmpl.Option("missingkey=error")
	}

	queries, err := template.ParseFS(queriesFS,
		"resources/queries/data/*.gosql",
		"resources/queries/inventory/*.gosql",
		"resources/queries/meta/*.gosql")

	if err != nil {
		log.Error(ctx, err, "failed to parse db schema from files")
		return nil, err
	}
	for _, tmpl := range schemas.Templates() {
		tmpl.Option("missingkey=error")
	}

	var conn *pgxpool.Pool

	if postgresParams.IsEmpty() {
		return nil, fmt.Errorf("no connection url for Postgres")
	}
	startTime := time.Now()
	log.Info(ctx, "Connecting to database...")
	connUrl, err := url.Parse(postgresParams.ConnStr)
	if err != nil {
		log.Error(ctx, err, "Error parsing PG connection url")
		return nil, err
	}
	queryValues := connUrl.Query()
	if postgresParams.SSLMode != "" {
		queryValues.Set("sslmode", postgresParams.SSLMode)
	} else {
		queryValues.Set("sslmode", "disable")
	}
	if postgresParams.CAFile != "" {
		queryValues.Set("sslrootcert", postgresParams.CAFile)
	}
	connUrl.RawQuery = queryValues.Encode()

	conn, err = pgxpool.New(ctx, connUrl.String())
	if err != nil {
		log.Error(ctx, err, "Unable to connect to database [%v]", postgresParams.ConnStr)
		return nil, err
	} else if err = conn.Ping(ctx); err != nil {
		log.Error(ctx, err, "Unable to connect to database [%v]", postgresParams.ConnStr)
		return nil, err
	} else {
		log.Info(ctx, "Connected to database [%v/%v] in %v",
			conn.Config().ConnConfig.Host, conn.Config().ConnConfig.Database, time.Since(startTime))
	}

	if !postgresParams.SkipMonitoring {
		collector := pgxpoolprometheus.NewCollector(conn, map[string]string{})
		err = prometheus.Register(collector)
	}

	return &Client{
		conn:    conn,
		schemas: schemas,
		queries: queries,
	}, err
}

func (c *Client) PrepareSchemaQuery(name string, args map[string]any) (string, error) {
	query := new(bytes.Buffer)
	if err := c.schemas.ExecuteTemplate(query, name, args); err != nil {
		return "", fmt.Errorf("cannot execute %s template: %w", name, err)
	}
	return query.String(), nil
}

func (c *Client) PrepareQuery(name string, args map[string]any) string {
	query := new(bytes.Buffer)
	if err := c.queries.ExecuteTemplate(query, name, args); err != nil {
		return ""
	}
	return query.String()
}

// CallsTable returns the name of the calls table using the given timestamp as a UNIX epoch suffix.
func CallsTable(timestamp time.Time) string {
	return fmt.Sprintf("calls_%d", GranularTs(timestamp, Granularity))
}

// TracesTable returns the name of the traces table using the given timestamp as a UNIX epoch suffix.
func TracesTable(timestamp time.Time) string {
	return fmt.Sprintf("traces_%d", GranularTs(timestamp, Granularity))
}

// SuspendTable returns the name of the suspend table using the given timestamp as a UNIX epoch suffix.
func SuspendTable(timestamp time.Time) string {
	return fmt.Sprintf("suspend_%d", GranularTs(timestamp, Granularity))
}

// DumpsTable returns the name of the dumps table using the given timestamp as a UNIX epoch suffix.
func DumpsTable(timestamp time.Time) string {
	return fmt.Sprintf("dump_objects_%d", GranularTs(timestamp, DumpsTableGranularity))
}

// InvertedIndexTable returns the name of the inverted index table using the prefix
// and the given timestamp as a UNIX epoch suffix.
func InvertedIndexTable(prefix string, timestamp time.Time) string {
	return fmt.Sprintf("i_%s_%d", prefix, timestamp.UTC().Unix())
}

// GranularTs returns the UNIX epoch of the timestamp truncated to the given granularity in UTC.
func GranularTs(timestamp time.Time, granularity time.Duration) int64 {
	return timestamp.UTC().Truncate(granularity).Unix()
}
