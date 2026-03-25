//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/maintenance"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MetadataRemoveTestSuite struct {
	suite.Suite
	pg  *helpers.PostgresContainer
	job maintenance.MaintenanceJob
	ctx context.Context

	ts                     time.Time
	podThatShouldBeRemoved *model.PodInfo
	podIsStillActive       *model.PodInfo
	existTempTables        map[string]*inventory.TempTableInfo
}

func (suite *MetadataRemoveTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)

	suite.pg = helpers.CreatePgContainer(suite.ctx)
	suite.job = maintenance.MaintenanceJob{
		Postgres:    suite.pg.Client,
		MinioClient: nil, // minio is not used for remove job
		JobConfig: &config.JobConfig{
			MetadataRemoval: 2,
		},
	}
}

func (suite *MetadataRemoveTestSuite) SetupTest() {
	suite.ts = time.Date(2024, 6, 25, 10, 0, 0, 0, time.UTC)
	var err error
	if suite.podThatShouldBeRemoved, _, err = suite.pg.AddPodsWithRestarts(suite.ctx, "ns1", "svc1",
		time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 25, 7, 0, 0, 0, time.UTC)); err != nil {
		log.Error(suite.ctx, err, "error pod info for pod, that should be removed")
		suite.FailNow("setup sub test")
	}

	if suite.podIsStillActive, _, err = suite.pg.AddPodsWithRestarts(suite.ctx, "ns2", "svc2",
		time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 25, 10, 0, 0, 0, time.UTC)); err != nil {
		log.Error(suite.ctx, err, "error pod info for pod, that should be removed")
		suite.FailNow("setup sub test")
	}
}

func (suite *MetadataRemoveTestSuite) TestRemoveTablesForSpecifiedTimeRange() {
	// Contains two pods, that are restarted every 30 minutes
	// ns1/svc1 pod exists since 00:00 to 07:00
	// ns2/svc2 pod exists since 00:00 to 10:00
	// Now is 10:00
	// Pod svc1 should be removed with restarts, because it has too old last_active field
	// Pod svc2 should be not removed (includes it's old restarts)
	t := suite.T()

	removeJob, err := maintenance.NewMetadataRemoveJob(suite.ctx, &suite.job, suite.ts)
	require.NoError(t, err)

	err = removeJob.Execute(suite.ctx)
	require.NoError(t, err)

	// Check pod for ns1/svc1
	pods, err := suite.pg.Client.GetUniquePodsForNamespaceActiveAfter(suite.ctx, suite.podThatShouldBeRemoved.Namespace, time.Time{})
	require.NoError(t, err)
	require.Equal(t, 0, len(pods))

	podResrarts, err := suite.pg.Client.GetPodRestarts(suite.ctx, suite.podThatShouldBeRemoved.Namespace, suite.podThatShouldBeRemoved.ServiceName, suite.podThatShouldBeRemoved.PodName)
	require.NoError(t, err)
	require.Equal(t, 0, len(podResrarts))

	// Check pod for ns2/svc2
	pods, err = suite.pg.Client.GetUniquePodsForNamespaceActiveAfter(suite.ctx, suite.podIsStillActive.Namespace, time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, len(pods))

	podResrarts, err = suite.pg.Client.GetPodRestarts(suite.ctx, suite.podIsStillActive.Namespace, suite.podIsStillActive.ServiceName, suite.podIsStillActive.PodName)
	require.NoError(t, err)
	require.Equal(t, 21, len(podResrarts))
}

func (suite *MetadataRemoveTestSuite) TearDownTest() {
	if err := suite.pg.CleanUpPods(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error cleaning up pods")
		suite.FailNow("tear down test")
	}
}

func (suite *MetadataRemoveTestSuite) TearDownSuite() {
	if err := suite.pg.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating pg container")
		suite.FailNow("tear down")
	}
}

func TestMetadataRemoveTestSuite(t *testing.T) {
	suite.Run(t, new(MetadataRemoveTestSuite))
}

