//go:build integration

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TransactionTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *TransactionTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *TransactionTestSuite) SetupTest() {
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *TransactionTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
}

func (suite *TransactionTestSuite) TestTransacionCommit() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 20, 00, 00, 00, 00, time.UTC)
	lastActive := time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC)

	var results model.StoreDumpResult
	pod := model.Pod{
		Namespace:   namespace,
		ServiceName: serviceName,
		PodName:     podName,
		RestartTime: restartTime,
	}

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          pod,
			CreationTime: time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	err := suite.db.Transaction(suite.ctx, func(tx db.DumpDbClient) error {
		var err error
		results, err = tx.StoreDumpsTransactionally(suite.ctx, dumpsInfo, nil, restartTime)
		if err != nil {
			return err
		}
		return nil
	})
	handle := fmt.Sprintf("%s-heap-%d", podName, time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC).UnixMilli())
	require.NoError(t, err)
	foundPod, err := suite.db.FindPod(suite.ctx, namespace, serviceName, podName)
	require.NoError(t, err)
	require.Equal(t, namespace, foundPod.Namespace)
	require.Equal(t, serviceName, foundPod.ServiceName)
	require.Equal(t, podName, foundPod.PodName)
	require.Equal(t, restartTime, foundPod.RestartTime)
	require.Equal(t, lastActive, *foundPod.LastActive)
	require.Equal(t, int64(1), results.HeapDumpsInserted)
	require.Equal(t, int64(1), results.PodsCreated)
	heapDump, err := suite.db.FindHeapDump(suite.ctx, handle)
	require.NoError(t, err)
	require.Equal(t, foundPod.Id, heapDump.PodId)
}

func (suite *TransactionTestSuite) TestTransacionRollback() {
	t := suite.T()

	namespace := "test-namespace"
	serviceName := "test-service"
	podName := "test-pod-78c95d6fc6-wfjkp"
	restartTime := time.Date(2024, 07, 21, 00, 00, 00, 00, time.UTC)

	var results model.StoreDumpResult
	pod := model.Pod{
		Namespace:   namespace,
		ServiceName: serviceName,
		PodName:     podName,
		RestartTime: restartTime,
	}

	dumpsInfo := []model.DumpInfo{
		{
			Pod:          pod,
			CreationTime: time.Date(2024, 07, 22, 03, 00, 00, 00, time.UTC),
			FileSize:     100,
			DumpType:     model.HeapDumpType,
		},
	}

	err := suite.db.Transaction(suite.ctx, func(tx db.DumpDbClient) error {
		var err error
		results, err = tx.StoreDumpsTransactionally(suite.ctx, dumpsInfo, nil, restartTime)
		if err != nil {
			return err
		}

		if _, err := suite.db.FindPod(suite.ctx, namespace, serviceName, "unexist"); err != nil {
			return err
		}
		return nil
	})
	require.Equal(t, int64(1), results.HeapDumpsInserted)
	require.Equal(t, int64(1), results.PodsCreated)
	require.Equal(t, int64(1), results.TimelinesCreated)
	require.ErrorContains(t, err, "record not found")

	_, err = suite.db.FindPod(suite.ctx, namespace, serviceName, podName)
	require.ErrorContains(t, err, "record not found")

	_, err = suite.db.FindTimeline(suite.ctx, restartTime)
	require.ErrorContains(t, err, "record not found")
}

func TestTransactionTestSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

