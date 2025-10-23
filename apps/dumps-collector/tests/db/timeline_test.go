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

type TimelineTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DbClient
}

func (suite *TimelineTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *TimelineTestSuite) SetupTest() {
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *TimelineTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
}

func (suite *TimelineTestSuite) TestCreateTimeline() {
	t := suite.T()

	curTime := time.Date(2024, 07, 10, 00, 30, 00, 00, time.UTC)

	timeline, isCreated, err := suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)
	require.True(t, isCreated)
	require.Equal(t, time.Date(2024, 07, 10, 00, 00, 00, 00, time.UTC), timeline.TsHour)
	require.Equal(t, model.RawStatus, timeline.Status)
	require.True(t, suite.db.HasTable(suite.ctx, "dump_objects_1720569600"))

	timeline2, isCreated, err := suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, timeline, timeline2)
}

func (suite *TimelineTestSuite) TestFindTimeline() {
	t := suite.T()

	timeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, time.Date(2024, 07, 21, 00, 30, 00, 00, time.UTC))
	require.NoError(t, err)

	foundTimeline, err := suite.db.FindTimeline(suite.ctx, time.Date(2024, 07, 21, 00, 40, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, timeline, foundTimeline)

	foundTimeline, err = suite.db.FindTimeline(suite.ctx, time.Date(2024, 07, 21, 10, 40, 00, 00, time.UTC))
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundTimeline)
}

func (suite *TimelineTestSuite) TestSearchTimelines() {
	t := suite.T()

	timeline1, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, time.Date(2024, 07, 26, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	timeline2, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, time.Date(2024, 07, 26, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	timeline3, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, time.Date(2024, 07, 26, 02, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	timelines, err := suite.db.SearchTimelines(suite.ctx, time.Date(2024, 07, 26, 00, 00, 00, 00, time.UTC), time.Date(2024, 07, 26, 02, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 3, len(timelines))
	require.Contains(t, timelines, *timeline1)
	require.Contains(t, timelines, *timeline2)
	require.Contains(t, timelines, *timeline3)

	timelines, err = suite.db.SearchTimelines(suite.ctx, time.Date(2024, 07, 26, 00, 30, 00, 00, time.UTC), time.Date(2024, 07, 26, 01, 30, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(timelines))
	require.Contains(t, timelines, *timeline2)

	timelines, err = suite.db.SearchTimelines(suite.ctx, time.Date(2024, 07, 26, 03, 00, 00, 00, time.UTC), time.Date(2024, 07, 26, 04, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, len(timelines))
}

func (suite *TimelineTestSuite) TestUpdateTimelineStatus() {
	t := suite.T()

	curTime := time.Date(2024, 07, 21, 04, 00, 00, 00, time.UTC)
	_, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	foundTimeline, err := suite.db.FindTimeline(suite.ctx, curTime)
	require.NoError(t, err)
	require.Equal(t, model.RawStatus, foundTimeline.Status)

	updatedTimeline, err := suite.db.UpdateTimelineStatus(suite.ctx, curTime, model.ZippingStatus)
	require.NoError(t, err)
	require.Equal(t, model.ZippingStatus, updatedTimeline.Status)

	foundTimeline, err = suite.db.FindTimeline(suite.ctx, curTime)
	require.NoError(t, err)
	require.Equal(t, model.ZippingStatus, foundTimeline.Status)
}

func (suite *TimelineTestSuite) TestRemoveTimeline() {
	t := suite.T()

	curTime := time.Date(2024, 07, 21, 07, 00, 00, 00, time.UTC)
	timeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx, curTime)
	require.NoError(t, err)

	foundTimeline, err := suite.db.FindTimeline(suite.ctx, curTime)
	require.NoError(t, err)
	require.Equal(t, timeline, foundTimeline)

	removedTimeline, err := suite.db.RemoveTimeline(suite.ctx, curTime)
	require.NoError(t, err)
	require.Equal(t, timeline, removedTimeline)

	foundTimeline, err = suite.db.FindTimeline(suite.ctx, curTime)
	require.ErrorContains(t, err, "record not found")
	require.Nil(t, foundTimeline)
}

func TestTimelineTestSuite(t *testing.T) {
	suite.Run(t, new(TimelineTestSuite))
}

