package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PgOperationLabelType string

const (
	// operation label for cloud_profiler_dumps_collector-go db client metrics
	pgOperationLabelName   = "operation"
	PgOperationCreateOne   = PgOperationLabelType("create-one")
	PgOperationInsertMany  = PgOperationLabelType("insert-many")
	PgOperationUpdate      = PgOperationLabelType("update")
	PgOperationCount       = PgOperationLabelType("count")
	PgOperationGetById     = PgOperationLabelType("get-by-id")
	PgOperationSelectOne   = PgOperationLabelType("select-one")
	PgOperationSearchMany  = PgOperationLabelType("search-many")
	PgOperationStatistic   = PgOperationLabelType("statistic")
	PgOperationRemove      = PgOperationLabelType("remove")
	PgOperationTransaction = PgOperationLabelType("transaction")
)

var (
	// cloud_profiler_dumps_collector_pg_operation_seconds metric
	// supported labels:
	// * "entity": "pod", "timeline", "td-top-dumps" or "heap-dumps"
	// * "operation": "create-one", "insert-many", "update", "count", "get-by-id", "select-one", "search-many", "statistic" or "remove"
	// * "operation": "create-one", "insert-many", "update", "count", "get-by-id", "select-one", "search-many", "statistic", "remove" or "transaction"
	// * "result": "success" or "fail"
	pgOperationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cloud_profiler_dumps_collector_pg_operation",
			Help: "Processing postgres operation time in seconds for cloud-profiler-dumps-collector",
		},
		[]string{entityLabelName, resultLabelName, pgOperationLabelName},
	)

	// cloud_profiler_dumps_collector_operation_affected_count metric
	// supported labels:
	// * "entity": "pod", "timeline", "td-top-dumps" or "heap-dumps"
	// * "operation": "create-one", "insert-many", "update", "count", "get-by-id", "select-one", "search-many", "statistic" or "remove"
	// * "result": "success" or "fail"
	pgOperationAffectedEntitiesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cloud_profiler_dumps_collector_pg_operation_affected_count",
			Help: "Processing postgres operation entities affected count for cloud-profiler-dumps-collector",
		},
		[]string{entityLabelName, resultLabelName, pgOperationLabelName},
	)
)

func AddPgOperationMetricValue(entity EntityLabelType, operation PgOperationLabelType, duration time.Duration, affectedRows int64, isError bool) {
	pgOperationSeconds.With(prometheus.Labels{
		entityLabelName:      string(entity),
		pgOperationLabelName: string(operation),
		resultLabelName:      resultLabel(isError),
	}).Observe(duration.Seconds())
	pgOperationAffectedEntitiesCount.With(prometheus.Labels{
		entityLabelName:      string(entity),
		pgOperationLabelName: string(operation),
		resultLabelName:      resultLabel(isError),
	}).Add(float64(affectedRows))
}
