//go:build integration

package integration

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/stretchr/testify/require"
)

func (suite *PGTestSuite) TestInsertPod() {
	t := suite.T()

	pod := model.PodInfo{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000000",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000000",
		LastRestart: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 27, 0, 0, 0, 0, time.Local),
	}

	err := suite.pg.Client.InsertPod(suite.ctx, pod)
	require.NoError(t, err)

	err = suite.pg.Client.InsertPod(suite.ctx, pod)
	require.ErrorContains(t, err, "duplicate key value violates unique constraint")

	pods, err := suite.pg.Client.GetUniquePodsForNamespaceActiveAfter(suite.ctx, "ns", time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, len(pods))
	require.Equal(t, pod, *pods[0])
}

func (suite *PGTestSuite) TestRemovePod() {
	t := suite.T()

	pod1 := model.PodInfo{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000001",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000001",
		LastRestart: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 27, 0, 0, 0, 0, time.Local),
	}

	pod2 := model.PodInfo{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000002",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000002",
		LastRestart: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
	}

	err := suite.pg.Client.InsertPod(suite.ctx, pod1)
	require.NoError(t, err)

	err = suite.pg.Client.InsertPod(suite.ctx, pod2)
	require.NoError(t, err)

	pods, err := suite.pg.Client.GetUniquePodsForNamespaceActiveBefore(suite.ctx, "ns", time.Date(2024, 6, 10, 0, 0, 0, 0, time.Local))
	require.NoError(t, err)
	require.Equal(t, 1, len(pods))
	require.Equal(t, pod2, *pods[0])

	err = suite.pg.Client.RemovePod(suite.ctx, pod2.PodId)
	require.NoError(t, err)

	pods, err = suite.pg.Client.GetUniquePodsForNamespaceActiveBefore(suite.ctx, "ns", time.Date(2024, 6, 10, 0, 0, 0, 0, time.Local))
	require.NoError(t, err)
	require.Equal(t, 0, len(pods))
}

func (suite *PGTestSuite) TestInsertPodRestarts() {
	t := suite.T()

	podRestart1 := model.PodRestart{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000001",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000001",
		RestartTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
	}

	podRestart2 := model.PodRestart{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000002",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000002",
		RestartTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
	}

	err := suite.pg.Client.InsertPodRestart(suite.ctx, podRestart1)
	require.NoError(t, err)

	err = suite.pg.Client.InsertPodRestart(suite.ctx, podRestart1)
	require.ErrorContains(t, err, "duplicate key value violates unique constraint")

	err = suite.pg.Client.InsertPodRestart(suite.ctx, podRestart2)
	require.NoError(t, err)

	podRestarts, err := suite.pg.Client.GetPodRestarts(suite.ctx, "ns", "svc", "svc-aaaaaaaaa-00000_0000000001")
	require.NoError(t, err)
	require.Equal(t, 1, len(podRestarts))
	require.Equal(t, podRestart1, *podRestarts[0])

	podRestarts, err = suite.pg.Client.GetPodRestarts(suite.ctx, "ns", "svc", "svc-aaaaaaaaa-00000_0000000002")
	require.NoError(t, err)
	require.Equal(t, 1, len(podRestarts))
	require.Equal(t, podRestart2, *podRestarts[0])

	podRestarts, err = suite.pg.Client.GetPodRestarts(suite.ctx, "ns", "svc", "unexist")
	require.NoError(t, err)
	require.Equal(t, 0, len(podRestarts))
}

func (suite *PGTestSuite) TestRemovePodRestarts() {
	t := suite.T()

	podRestart1 := model.PodRestart{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000000",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000000",
		RestartTime: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
	}

	podRestart2 := model.PodRestart{
		PodId:       "ns.svc-aaaaaaaaa-00000_0000000000",
		Namespace:   "ns",
		ServiceName: "svc",
		PodName:     "svc-aaaaaaaaa-00000_0000000000",
		RestartTime: time.Date(2024, 6, 2, 0, 0, 0, 0, time.Local),
		ActiveSince: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local),
		LastActive:  time.Date(2024, 6, 2, 0, 0, 0, 0, time.Local),
	}

	err := suite.pg.Client.InsertPodRestart(suite.ctx, podRestart1)
	require.NoError(t, err)

	err = suite.pg.Client.InsertPodRestart(suite.ctx, podRestart2)
	require.NoError(t, err)

	podRestarts, err := suite.pg.Client.GetPodRestarts(suite.ctx, "ns", "svc", "svc-aaaaaaaaa-00000_0000000000")
	require.NoError(t, err)
	require.Equal(t, 2, len(podRestarts))

	err = suite.pg.Client.RemovePodRestart(suite.ctx, "ns.svc-aaaaaaaaa-00000_0000000000")
	require.NoError(t, err)

	podRestarts, err = suite.pg.Client.GetPodRestarts(suite.ctx, "ns", "svc", "svc-aaaaaaaaa-00000_0000000000")
	require.NoError(t, err)
	require.Equal(t, 0, len(podRestarts))
}
