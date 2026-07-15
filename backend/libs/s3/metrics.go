package s3

import "github.com/prometheus/client_golang/prometheus"

const (
	// operation type label for cdt_minio_operation_latency_seconds: get, list, put, remove, remove_many
	operationTypeLabelName  = "operation"
	operationTypeGet        = "get"
	operationTypeList       = "list"
	operationTypePut        = "put"
	operationTypeRemove     = "remove"
	operationTypeRemoveMany = "remove_many"
)

var (
	// cdt_minio_operation_latency_seconds metric
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cdt_minio_operation_latency_seconds",
			Help: "Processing minio operation time in seconds",
		},
		[]string{operationTypeLabelName},
	)

	// cdt_minio_operation_objects_count metric
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioObjectsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_minio_operation_objects_count",
			Help: "Processing minio objects count",
		},
		[]string{operationTypeLabelName},
	)

	// cdt_minio_operation_errors_count classes every failed minio operation by
	// operation type, so the S3-error rate is a first-class alerting signal
	// instead of a line lost in the logs.
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioErrorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_minio_operation_errors_count",
			Help: "Failed minio operations count, by operation type",
		},
		[]string{operationTypeLabelName},
	)
)

// Collectors returns the cdt_minio_* collectors so a caller can register them
// on its own registry. The profiler-backend subcommands each expose a private
// registry (never the Prometheus default), so without this seam the S3 series
// would be invisible on their /metrics. registerMetrics still registers the
// same collectors on the default registry for the legacy apps/maintenance path.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		operationMinioLatencySeconds,
		operationMinioObjectsCount,
		operationMinioErrorsCount,
	}
}

// operationTypes lists every operation label value, so RegisterMetrics can
// materialize each series up front: a *Vec with no observed children is
// invisible to Gather, and dashboards want a stable zero, not a series that
// only appears on the first request or the first error.
var operationTypes = []string{
	operationTypeGet, operationTypeList, operationTypePut,
	operationTypeRemove, operationTypeRemoveMany,
}

// RegisterMetrics registers the cdt_minio_* collectors on reg and initializes
// every operation series to zero. It is safe to call more than once and
// tolerates a collector already registered on reg (prometheus.
// AlreadyRegisteredError), so several MinioClients sharing one process registry
// do not fight over the series.
func RegisterMetrics(reg prometheus.Registerer) {
	for _, c := range Collectors() {
		if err := reg.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				panic(err)
			}
		}
	}
	for _, op := range operationTypes {
		labels := prometheus.Labels{operationTypeLabelName: op}
		operationMinioLatencySeconds.With(labels)
		operationMinioObjectsCount.With(labels)
		operationMinioErrorsCount.With(labels)
	}
}

func registerMetrics() {
	prometheus.Register(operationMinioLatencySeconds)
	prometheus.Register(operationMinioObjectsCount)
	prometheus.Register(operationMinioErrorsCount)
}

func ObserveOperation(seconds float64, objectsCount int, operationType string) {
	operationMinioLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationType,
	}).Observe(seconds)
	operationMinioObjectsCount.With(prometheus.Labels{
		operationTypeLabelName: operationType,
	}).Add(float64(objectsCount))
}

// ObserveError counts one failed minio operation of the given type.
func ObserveError(operationType string) {
	operationMinioErrorsCount.With(prometheus.Labels{
		operationTypeLabelName: operationType,
	}).Inc()
}
