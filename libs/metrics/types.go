package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	pgOperations interface {
		ReadCalls(startTime time.Time, namespace string, serviceName string, err error)
		ReadDumps(startTime time.Time, namespace string, serviceName string, err error)
	}
	s3Operations interface {
		WriteCalls(startTime time.Time, namespace string, err error)
		WriteDumps(startTime time.Time, namespace string, err error)
		AddCallsDataRowsCount(rowsCount int, namespace string)
		AddDumpsDataRowsCount(rowsCount int, namespace string)
		AddCallsDataSizeBytes(dataSize int64, namespace string)
		AddDumpsDataSizeBytes(dataSize int64, namespace string)
	}

	fileOperations interface {
		WriteCalls(startTime time.Time, namespace string, serviceName string, err error)
		WriteDumps(startTime time.Time, namespace string, serviceName string, err error)
	}
)

func (pi pgImpl) ReadCalls(startTime time.Time, namespace string, serviceName string, err error) {
	pi.ReadCallsOperationTime(time.Since(startTime).Seconds(), namespace, serviceName, err)
}

func (pi pgImpl) ReadCallsOperationTime(seconds float64, namespace string, serviceName string, err error) {
	operationPGLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeRead,
		dataTypeLabelName:      dataTypeCalls,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
		serviceLabelName:       serviceName,
	}).Observe(seconds)
}

func (pi pgImpl) ReadDumps(startTime time.Time, namespace string, serviceName string, err error) {
	pi.ReadDumpsOperationTime(time.Since(startTime).Seconds(), namespace, serviceName, err)
}

func (pi pgImpl) ReadDumpsOperationTime(seconds float64, namespace string, serviceName string, err error) {
	operationPGLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeRead,
		dataTypeLabelName:      dataTypeDumps,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
		serviceLabelName:       serviceName,
	}).Observe(seconds)
}

func (si s3Impl) WriteCalls(startTime time.Time, namespace string, err error) {
	si.WriteCallsOperationTime(time.Since(startTime).Seconds(), namespace, err)
}

func (si s3Impl) WriteCallsOperationTime(seconds float64, namespace string, err error) {
	operationS3LatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeWrite,
		dataTypeLabelName:      dataTypeCalls,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
	}).Observe(seconds)
}

func (si s3Impl) WriteDumps(startTime time.Time, namespace string, err error) {
	si.WriteDumpsOperationTime(time.Since(startTime).Seconds(), namespace, err)
}

func (si s3Impl) WriteDumpsOperationTime(seconds float64, namespace string, err error) {
	operationS3LatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeWrite,
		dataTypeLabelName:      dataTypeDumps,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
	}).Observe(seconds)
}

func (si s3Impl) AddCallsDataRowsCount(rowsCount int, namespace string) {
	dataRowsCount.With(prometheus.Labels{
		dataTypeLabelName:  dataTypeCalls,
		namespaceLabelName: namespace,
	}).Add(float64(rowsCount))
}

func (si s3Impl) AddDumpsDataRowsCount(rowsCount int, namespace string) {
	dataRowsCount.With(prometheus.Labels{
		dataTypeLabelName:  dataTypeDumps,
		namespaceLabelName: namespace,
	}).Add(float64(rowsCount))
}

func (si s3Impl) AddCallsDataSizeBytes(dataSize int64, namespace string) {
	dataFileSize.With(prometheus.Labels{
		dataTypeLabelName:  dataTypeCalls,
		namespaceLabelName: namespace,
	}).Add(float64(dataSize))
}

func (si s3Impl) AddDumpsDataSizeBytes(dataSize int64, namespace string) {
	dataFileSize.With(prometheus.Labels{
		dataTypeLabelName:  dataTypeDumps,
		namespaceLabelName: namespace,
	}).Add(float64(dataSize))
}

func (ti tempFileImpl) WriteCalls(startTime time.Time, namespace string, serviceName string, err error) {
	ti.WriteCallsOperationTime(time.Since(startTime).Seconds(), namespace, serviceName, err)
}

func (ti tempFileImpl) WriteCallsOperationTime(seconds float64, namespace string, serviceName string, err error) {
	operationTempLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeWrite,
		dataTypeLabelName:      dataTypeCalls,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
		serviceLabelName:       serviceName,
	}).Observe(seconds)
}

func (ti tempFileImpl) WriteDumps(startTime time.Time, namespace string, serviceName string, err error) {
	ti.WriteDumpsOperationTime(time.Since(startTime).Seconds(), namespace, serviceName, err)
}

func (ti tempFileImpl) WriteDumpsOperationTime(seconds float64, namespace string, serviceName string, err error) {
	operationTempLatencySeconds.With(prometheus.Labels{
		operationTypeLabelName: operationTypeWrite,
		dataTypeLabelName:      dataTypeDumps,
		resultLabelName:        resultLabel(err),
		namespaceLabelName:     namespace,
		serviceLabelName:       serviceName,
	}).Observe(seconds)
}

func resultLabel(err error) string {
	if err != nil {
		return resultFail
	}
	return resultSuccess
}
