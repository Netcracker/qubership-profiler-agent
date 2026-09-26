package s3

import (
	"context"
	"errors"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
)

// OperationLabel is the label that carries the operation on every
// cdt_minio_* series. A query that groups or filters these series by
// operation uses this name.
const OperationLabel = "operation"

// Values of [OperationLabel].
const (
	OperationGet        = "get"
	OperationList       = "list"
	OperationPut        = "put"
	OperationRemove     = "remove"
	OperationRemoveMany = "remove_many"
)

var (
	// cdt_minio_operation_latency_seconds observes one sample per completed
	// request, a NoSuchKey answer included. For "get" that is the HEAD that
	// opens an object, every ranged read of it, and every whole-object GET.
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cdt_minio_operation_latency_seconds",
			Help: "Processing minio operation time in seconds",
		},
		[]string{OperationLabel},
	)

	// cdt_minio_operation_objects_count counts the objects a completed request
	// touched. For "get" it counts objects opened or fetched, not the ranged
	// reads of an opened object.
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioObjectsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_minio_operation_objects_count",
			Help: "Processing minio objects count",
		},
		[]string{OperationLabel},
	)

	// cdt_minio_operation_errors_count classes every failed minio operation by
	// operation type, so the S3-error rate is a first-class alerting signal
	// instead of a line lost in the logs. A failed ranged read counts once per
	// object, and a request the caller cancelled does not count.
	// supported labels:
	// * "operation": "get", "list", "put", "remove" or "remove_many"
	operationMinioErrorsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdt_minio_operation_errors_count",
			Help: "Failed minio operations count, by operation type",
		},
		[]string{OperationLabel},
	)
)

// Collectors returns the cdt_minio_* collectors so a caller can register them
// on its own registry. The profiler-backend subcommands each expose a private
// registry (never the Prometheus default), so without this seam the S3 series
// would be invisible on their /metrics. registerMetrics still registers the
// same collectors on the default registry for callers that scrape it.
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
	OperationGet, OperationList, OperationPut,
	OperationRemove, OperationRemoveMany,
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
		labels := prometheus.Labels{OperationLabel: op}
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
		OperationLabel: operationType,
	}).Observe(seconds)
	operationMinioObjectsCount.With(prometheus.Labels{
		OperationLabel: operationType,
	}).Add(float64(objectsCount))
}

// ObserveError counts one failed minio operation of the given type.
func ObserveError(operationType string) {
	operationMinioErrorsCount.With(prometheus.Labels{
		OperationLabel: operationType,
	}).Inc()
}

// ObserveResult records the outcome of one S3 request that started at start.
// A nil err observes the latency and adds objects to the objects count. An
// err that is [context.Canceled] or [context.DeadlineExceeded] while ctx is
// done records nothing, because the caller abandoned the request. Any other
// err, a deadline the transport hit under a live ctx included, counts one
// error.
func ObserveResult(ctx context.Context, operation string, start time.Time, objects int, err error) {
	switch {
	case err == nil:
		ObserveOperation(time.Since(start).Seconds(), objects, operation)
	case ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		// Abandoned: neither a completed request nor an S3 failure.
	default:
		ObserveError(operation)
	}
}

// IsNotFound reports whether err is an S3 NoSuchKey answer.
func IsNotFound(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}
