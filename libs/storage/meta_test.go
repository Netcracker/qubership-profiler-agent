package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPodInfo(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)

	t.Run("pod info", func(t *testing.T) {
		pod := &PodInfo{
			PodId:       "id",
			Namespace:   "ns",
			ServiceName: "svc",
			PodName:     "pod",
			ActiveSince: ts,
			LastRestart: ts,
			LastActive:  ts,
			Tags:        map[string]string{"type": "java"},
		}
		assert.Equal(t, "svc/pod", pod.String())
	})

	t.Run("pod restart", func(t *testing.T) {
		pod := &PodRestart{
			PodId:       "id",
			Namespace:   "ns",
			ServiceName: "svc",
			PodName:     "pod",
			RestartTime: ts,
			ActiveSince: ts,
			LastActive:  ts,
		}
		assert.Equal(t, "svc/pod/1708509720000", pod.String())
	})
}
