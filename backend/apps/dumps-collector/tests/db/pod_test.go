//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PodTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DbClient
}

func (suite *PodTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *PodTestSuite) SetupTest() {
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *PodTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
}

func (suite *PodTestSuite) TestCreatePod() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 20, 00, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)
	require.Equal(t, namespace, pod.Namespace)
	require.Equal(t, serviceName, pod.ServiceName)
	require.Equal(t, podName, pod.PodName)
	require.Equal(t, restartTime, pod.RestartTime)
	require.Nil(t, pod.LastActive)
}

func (suite *PodTestSuite) TestGetPodsCount() {
	t := suite.T()

	podsCount, err := suite.db.GetPodsCount(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), podsCount)

	_, _, err = suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "service-0", "pod-0", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	_, _, err = suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "service-1", "pod-1", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	_, _, err = suite.db.CreatePodIfNotExist(suite.ctx, "ns-1", "service-1", "pod-1", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	podsCount, err = suite.db.GetPodsCount(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), podsCount)
}

func (suite *PodTestSuite) TestCreateAlreadyExistedPod() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 20, 01, 00, 00, 00, time.UTC)

	pod1, isCreated, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)
	require.True(t, isCreated)

	pod2, isCreated, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, pod1.Id, pod2.Id)

	podName = "test-pod-78c95d6fc6-wfjkk"
	pod3, isCreated, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)
	require.True(t, isCreated)
	require.NotEqual(t, pod1.Id, pod3.Id)
}

func (suite *PodTestSuite) TestFindPod() {
	t := suite.T()

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "test-namespace", "test-service", "test-pod", time.Date(2024, 07, 21, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	foundPod, err := suite.db.FindPod(suite.ctx, "test-namespace", "test-service", "test-pod")
	require.NoError(t, err)
	require.Equal(t, pod, foundPod)

	foundPod, err = suite.db.FindPod(suite.ctx, "test-namespace", "test-service", "unexist")
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundPod)
}

func (suite *PodTestSuite) TestSearchPods() {
	t := suite.T()

	pod1, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "service-0", "pod-0", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	pod2, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-0", "service-1", "pod-1", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	pod3, _, err := suite.db.CreatePodIfNotExist(suite.ctx, "ns-1", "service-1", "pod-1", time.Date(2024, 07, 21, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Find pods on ns-0
	ns0PodFilter := model.NewPodFilterComparator("namespace", model.ComparatorEqual, "ns-0")
	pods, err := suite.db.SearchPods(suite.ctx, ns0PodFilter)
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	require.Contains(t, pods, *pod1)
	require.Contains(t, pods, *pod2)

	// Find pods on svc-0
	svc0PodFilter := model.NewPodFilterComparator("service_name", model.ComparatorEqual, "service-0")
	pods, err = suite.db.SearchPods(suite.ctx, svc0PodFilter)
	require.NoError(t, err)
	require.Equal(t, 1, len(pods))
	require.Contains(t, pods, *pod1)

	// Find pods on ns-1
	ns1PodFilter := model.NewPodFilterComparator("namespace", model.ComparatorEqual, "ns-1")

	// Find pods on svc-1
	svc1PodFilter := model.NewPodFilterComparator("service_name", model.ComparatorEqual, "service-1")

	// Find pods on ns-0 and service-0 or ns-1 and service-1
	podFilter := model.NewPodFilterСondition(model.OperationOr,
		model.NewPodFilterСondition(model.OperationAnd, ns0PodFilter, svc0PodFilter),
		model.NewPodFilterСondition(model.OperationAnd, ns1PodFilter, svc1PodFilter),
	)

	pods, err = suite.db.SearchPods(suite.ctx, podFilter)
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	require.Contains(t, pods, *pod1)
	require.Contains(t, pods, *pod3)
}

func (suite *PodTestSuite) TestUpdatePodLastActive() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC)

	pod, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace, serviceName, podName, restartTime)
	require.NoError(t, err)

	lastActive := time.Date(2024, 07, 21, 04, 00, 00, 00, time.UTC)

	updatedPod, err := suite.db.UpdatePodLastActive(suite.ctx, namespace, serviceName, podName, restartTime, lastActive)
	require.NoError(t, err)
	require.Equal(t, pod.Id, updatedPod.Id)
	require.Equal(t, lastActive, *updatedPod.LastActive)

	foundPod, err := suite.db.FindPod(suite.ctx, namespace, serviceName, podName)
	require.NoError(t, err)
	require.Equal(t, pod.Id, foundPod.Id)
	require.Equal(t, lastActive, *foundPod.LastActive)

	lastActive = time.Date(2024, 07, 21, 03, 00, 00, 00, time.UTC)
	updatedPod, err = suite.db.UpdatePodLastActive(suite.ctx, namespace, serviceName, podName, restartTime, lastActive)
	require.NoError(t, err)
	require.Equal(t, foundPod, updatedPod)
}

func (suite *PodTestSuite) TestRemoveOldPods() {
	t := suite.T()

	namespace1 := "test-namespace"
	serviceName1 := "test-service"
	podName1 := "test-pod-78c95d6fc6-wfjkp"
	restartTime1 := time.Date(2024, 07, 22, 05, 00, 00, 00, time.UTC)

	_, _, err := suite.db.CreatePodIfNotExist(suite.ctx, namespace1, serviceName1, podName1, restartTime1)
	require.NoError(t, err)

	namespace2 := "test-namespace"
	serviceName2 := "test-service"
	podName2 := "test-pod-78c95d6fc6-wsdgh"
	restartTime2 := time.Date(2024, 07, 22, 05, 00, 00, 00, time.UTC)

	_, _, err = suite.db.CreatePodIfNotExist(suite.ctx, namespace2, serviceName2, podName2, restartTime2)
	require.NoError(t, err)

	lastActive1 := time.Date(2024, 07, 22, 05, 00, 00, 00, time.UTC)
	pod1, err := suite.db.UpdatePodLastActive(suite.ctx, namespace1, serviceName1, podName1, restartTime1, lastActive1)
	require.NoError(t, err)

	lastActive2 := time.Date(2024, 07, 22, 06, 00, 00, 00, time.UTC)
	pod2, err := suite.db.UpdatePodLastActive(suite.ctx, namespace2, serviceName2, podName2, restartTime2, lastActive2)
	require.NoError(t, err)

	removedPods, err := suite.db.RemoveOldPods(suite.ctx, time.Date(2024, 07, 22, 05, 30, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(removedPods))
	require.Contains(t, removedPods, *pod1)

	foundPod, err := suite.db.FindPod(suite.ctx, namespace1, serviceName1, podName1)
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundPod)

	foundPod, err = suite.db.FindPod(suite.ctx, namespace2, serviceName2, podName2)
	require.NoError(t, err)
	require.Equal(t, pod2, foundPod)
}

func TestPodTestSuite(t *testing.T) {
	suite.Run(t, new(PodTestSuite))
}

