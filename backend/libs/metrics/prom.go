package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// cdt_cloud_storage_pg_operation_latency_seconds metric
	// supported labels:
	// * "operation": "read" or "write"
	// * "data": "calls" or "dumps"
	// * "result": "success" or "fail"
	// * "namespace": the processed namespace
	// * "service": the processed service name
	operationPGLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cdt_cloud_storage_pg_operation_latency_seconds",
			Help: "Processing postgres operation time in seconds for compactor",
		},
		[]string{operationTypeLabelName, dataTypeLabelName, resultLabelName, namespaceLabelName, serviceLabelName},
	)

	// cdt_cloud_storage_s3_operation_latency_seconds metric
	// supported labels:
	// * "operation": "read" or "write"
	// * "data": "calls" or "dumps"
	// * "result": "success" or "fail"
	// * "namespace": the processed namespace
	operationS3LatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cdt_cloud_storage_s3_operation_latency_seconds",
			Help: "Processing s3 operation time in seconds for compactor",
		},
		[]string{operationTypeLabelName, dataTypeLabelName, resultLabelName, namespaceLabelName},
	)

	// cdt_cloud_storage_temp_file_operation_latency_seconds metric
	// supported labels:
	// * "data": "calls" or "dumps"
	// * "result": "success" or "fail"
	// * "namespace": the processed namespace
	// * "service": the processed service name
	operationTempLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cdt_cloud_storage_temp_file_operation_latency_seconds",
			Help: "Processing temp file operation time in seconds for compactor",
		},
		[]string{operationTypeLabelName, dataTypeLabelName, resultLabelName, namespaceLabelName, serviceLabelName},
	)

	// cdt_cloud_storage_data_rows_count metric
	// supported labels:
	// * "data": "calls" or "dumps"
	// * "namespace": the processed namespace
	dataRowsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_cloud_storage_data_rows_count",
			Help: "Processing rows count for compactor uploaded files",
		},
		[]string{dataTypeLabelName, namespaceLabelName},
	)

	// cdt_cloud_storage_data_size_bytes metric
	// supported labels:
	// * "data": "calls" or "dumps"
	// * "namespace": the processed namespace
	dataFileSize = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_cloud_storage_data_size_bytes",
			Help: "Processing data size for compactor uploaded files",
		},
		[]string{dataTypeLabelName, namespaceLabelName},
	)
)

// Register registers custom prometheus metrics
func Register() {
	prometheus.Register(operationPGLatencySeconds)
	prometheus.Register(operationS3LatencySeconds)
	prometheus.Register(operationTempLatencySeconds)
	prometheus.Register(dataRowsCount)
	prometheus.Register(dataFileSize)
}
