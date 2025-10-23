package streams

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestReadSuspend_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t1 := time.Date(2023, 7, 24, 12, 26, 17, 657000000, time.UTC)
	t2 := time.Date(2023, 7, 24, 12, 27, 52, 925000000, time.UTC)

	testSuspendFile := filepath.Join(ResourceDir, "test-service", "test-service.suspend.protocol")
	expectedSuspendLog := filepath.Join(ResourceDir, "test-service", "test-service.suspend.expected.txt")

	t.Run("suspend", func(t *testing.T) {
		c := testChunk(t, model.StreamSuspend, testSuspendFile)

		suspend, res, err := ReadSuspend(ctx, c)
		assert.Nil(t, err)
		//_ = os.WriteFile("test-service.suspend.expected.txt", []byte(res), 0644) // for debug

		assert.Equal(t, readTestFile(t, expectedSuspendLog), stripLines(res))
		assert.Equal(t, 539, len(suspend.List))
		assert.Equal(t, t1, suspend.StartTime)
		assert.Equal(t, t2, suspend.EndTime)

		t2 = t2.Add(time.Nanosecond)
		for i, a := range suspend.List {
			assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
			assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
		}
	})
}

func TestReadSuspend_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t1 := time.Date(2023, 8, 4, 16, 42, 6, 615000000, time.UTC)
	t2 := time.Date(2023, 8, 4, 16, 48, 0, 317000000, time.UTC)

	testSuspendFile := filepath.Join(ResourceDir, "u5min", "u5min-service.suspend.protocol")
	expectedSuspendLog := filepath.Join(ResourceDir, "u5min", "u5min-service.suspend.expected.txt")

	t.Run("suspend", func(t *testing.T) {
		c := testChunk(t, model.StreamSuspend, testSuspendFile)

		suspend, res, err := ReadSuspend(ctx, c)
		assert.Nil(t, err)
		//fmt.Print(res)
		//_ = os.WriteFile("u5min-service.suspend.expected.txt", []byte(res), 0644) // for debug
		assert.Equal(t, readTestFile(t, expectedSuspendLog), stripLines(res))
		assert.Equal(t, 638, len(suspend.List))
		assert.Equal(t, t1, suspend.StartTime)
		assert.Equal(t, t2, suspend.EndTime)

		t2 = t2.Add(time.Nanosecond)
		for i, a := range suspend.List {
			assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
			assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
		}
	})
}
