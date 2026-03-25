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
)

func registerMetrics() {
	prometheus.Register(operationMinioLatencySeconds)
	prometheus.Register(operationMinioObjectsCount)
}

func ObserveOperation(seconds float64, objectsCount int, operationType string) {
	operationMinioLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationType,
	}).Observe(seconds)
	operationMinioObjectsCount.With(prometheus.Labels{
		operationTypeLabelName: operationType,
	}).Add(float64(objectsCount))
}
