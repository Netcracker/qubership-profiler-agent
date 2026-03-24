//go:build unit

package task

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFromPodNameWithTs(t *testing.T) {
	t.Run("restricted symbols in pod name", func(t *testing.T) {
		podNameWithTs := "spring-boot-3-undertow?-5cbcd847d-l2t7t_1719318147399"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.Errorf(t, err, "does not match pod name with ts format")
		assert.Equal(t, "", serviceName)
		assert.Equal(t, "", podName)
		assert.Equal(t, time.Time{}, restartTime)
	})

	t.Run("no ts in pod name", func(t *testing.T) {
		podNameWithTs := "quarkus-3-vertx-b96c5d98c-dfxqg"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "quarkus-3-vertx", serviceName)
		assert.Equal(t, "quarkus-3-vertx-b96c5d98c-dfxqg", podName)
		assert.Equal(t, time.UnixMilli(0).UTC(), restartTime)
	})

	t.Run("pod name from deployment", func(t *testing.T) {
		podNameWithTs := "esc-test-service-574b7b674c-5pj4b_1721606400000"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "esc-test-service", serviceName)
		assert.Equal(t, "esc-test-service-574b7b674c-5pj4b_1721606400000", podName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), restartTime)
	})

	t.Run("pod name from daemon set", func(t *testing.T) {
		podNameWithTs := "ingress-nginx-controller-7sgvw_1721606400000"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "ingress-nginx-controller", serviceName)
		assert.Equal(t, "ingress-nginx-controller-7sgvw_1721606400000", podName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), restartTime)
	})

	t.Run("pod name from stateful set", func(t *testing.T) {
		podNameWithTs := "vault-service-0_1721606400000"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "vault-service", serviceName)
		assert.Equal(t, "vault-service-0_1721606400000", podName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), restartTime)
	})

	t.Run("pod name from deployment with numeric end", func(t *testing.T) {
		podNameWithTs := "esc-test-service-0-574b7b674c-5pj4b_1721606400000"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "esc-test-service-0", serviceName)
		assert.Equal(t, "esc-test-service-0-574b7b674c-5pj4b_1721606400000", podName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), restartTime)
	})

	//TODO: is it possible case?
	/*t.Run("pod name from ds with numeric end", func(t *testing.T) {
		podNameWithTs := "ingress-nginx-controller-1-7sgvw_1721606400000"
		serviceName, podName, restartTime, err := ParseFromPodNameWithTs(podNameWithTs)
		assert.NoError(t, err)
		assert.Equal(t, "ingress-nginx-controller-1", serviceName)
		assert.Equal(t, "ingress-nginx-controller-1-7sgvw", podName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00), restartTime)
	})*/
}

func TestParseDumpInfo(t *testing.T) {
	t.Run("relative path", func(t *testing.T) {
		path := "../ns/2024/07/28/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.td.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.ErrorContains(t, err, "does not match the dump file path format")
		assert.Nil(t, dumpInfo)
	})

	t.Run("missed directory", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.td.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.ErrorContains(t, err, "does not match the dump file path format")
		assert.Nil(t, dumpInfo)
	})

	t.Run("unexpected symbols time directory", func(t *testing.T) {
		path := "ns/2024/07/28/00T/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.td.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.ErrorContains(t, err, "incorrect hour directory 00T")
		assert.Nil(t, dumpInfo)
	})

	t.Run("pod name parsing issue", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000r/20240728T000000.td.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.ErrorContains(t, err, "does not match pod name with ts format")
		assert.Nil(t, dumpInfo)
	})

	t.Run("unexpected type", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.td.zip"
		dumpInfo, err := ParseDumpInfo(path)
		assert.ErrorContains(t, err, "has incorrect type")
		assert.Nil(t, dumpInfo)
	})

	t.Run("valid td file", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.td.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.NoError(t, err)
		require.NotNil(t, dumpInfo)
		assert.Equal(t, "ns", dumpInfo.Pod.Namespace)
		assert.Equal(t, "spring-boot-3-undertow", dumpInfo.Pod.ServiceName)
		assert.Equal(t, "spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000", dumpInfo.Pod.PodName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), dumpInfo.Pod.RestartTime)
		assert.Equal(t, time.Date(2024, 7, 28, 00, 00, 00, 00, time.UTC), dumpInfo.CreationTime)
		assert.Equal(t, model.TdDumpType, dumpInfo.DumpType)
	})

	t.Run("valid top file", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.top.txt"
		dumpInfo, err := ParseDumpInfo(path)
		assert.NoError(t, err)
		require.NotNil(t, dumpInfo)
		assert.Equal(t, "ns", dumpInfo.Pod.Namespace)
		assert.Equal(t, "spring-boot-3-undertow", dumpInfo.Pod.ServiceName)
		assert.Equal(t, "spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000", dumpInfo.Pod.PodName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), dumpInfo.Pod.RestartTime)
		assert.Equal(t, time.Date(2024, 7, 28, 00, 00, 00, 00, time.UTC), dumpInfo.CreationTime)
		assert.Equal(t, model.TopDumpType, dumpInfo.DumpType)
	})

	t.Run("valid heap file", func(t *testing.T) {
		path := "ns/2024/07/28/00/00/00/spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000/20240728T000000.hprof.zip"
		dumpInfo, err := ParseDumpInfo(path)
		assert.NoError(t, err)
		require.NotNil(t, dumpInfo)
		assert.Equal(t, "ns", dumpInfo.Pod.Namespace)
		assert.Equal(t, "spring-boot-3-undertow", dumpInfo.Pod.ServiceName)
		assert.Equal(t, "spring-boot-3-undertow-5cbcd847d-l2t7t_1721606400000", dumpInfo.Pod.PodName)
		assert.Equal(t, time.Date(2024, 7, 22, 00, 00, 00, 00, time.UTC), dumpInfo.Pod.RestartTime)
		assert.Equal(t, time.Date(2024, 7, 28, 00, 00, 00, 00, time.UTC), dumpInfo.CreationTime)
		assert.Equal(t, model.HeapDumpType, dumpInfo.DumpType)
	})
}

func TestParseTimeHour(t *testing.T) {
	t.Run("relative path", func(t *testing.T) {
		path := "../ns/2024/07/28"
		tHour, err := ParseTimeHour(path)
		assert.ErrorContains(t, err, "does not match the time hour path format")
		assert.Nil(t, tHour)
	})

	t.Run("missed directory", func(t *testing.T) {
		path := "ns/2024/07/28"
		tHour, err := ParseTimeHour(path)
		assert.ErrorContains(t, err, "does not match the time hour path format")
		assert.Nil(t, tHour)
	})

	t.Run("unexpected symbols time directory", func(t *testing.T) {
		path := "ns/2024/07/28/01T"
		tHour, err := ParseTimeHour(path)
		assert.ErrorContains(t, err, "incorrect hour directory")
		assert.Nil(t, tHour)
	})

	t.Run("valid dir", func(t *testing.T) {
		path := "ns/2024/07/28/01"
		tHour, err := ParseTimeHour(path)
		assert.NoError(t, err)
		require.NotNil(t, tHour)
		assert.Equal(t, time.Date(2024, 7, 28, 1, 0, 0, 0, time.UTC), *tHour)
	})

	t.Run("valid zip", func(t *testing.T) {
		path := "ns/2024/07/28/01.zip"
		tHour, err := ParseTimeHour(path)
		assert.NoError(t, err)
		require.NotNil(t, tHour)
		assert.Equal(t, time.Date(2024, 7, 28, 1, 0, 0, 0, time.UTC), *tHour)
	})
}
