package db

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/google/uuid"
)

// DbClient defines the interface for database operations
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

// DumpDbClient extends DbClient with transaction support and additional dump operations
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

// NewDumpDbClient creates a new database client
// This function is kept for backward compatibility and uses the factory in cmd package
// Deprecated: This function exists for backward compatibility but cannot create clients directly
// Use the factory in cmd/run.go instead
func NewDumpDbClient(ctx context.Context, params DBParams) (DumpDbClient, error) {
	return nil, fmt.Errorf("NewDumpDbClient is deprecated, use factory in cmd package")
}
