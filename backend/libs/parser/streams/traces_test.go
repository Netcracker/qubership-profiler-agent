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

func TestReadTrace_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.ERROR)
	t1 := time.Date(2023, 7, 24, 12, 25, 0, 0, time.UTC)
	t2 := time.Date(2023, 7, 24, 12, 29, 0, 0, time.UTC)

	testDictionaryFile := filepath.Join(ResourceDir, "test-service", "test-service.dictionary.protocol")
	testTraceFile := filepath.Join(ResourceDir, "test-service", "test-service.traces.0.protocol")
	expectedTraceLog := filepath.Join(ResourceDir, "test-service", "test-service.traces.expected.txt")

	cd := testChunk(t, model.StreamDictionary, testDictionaryFile)
	dict, _, err := ReadDictionary(ctx, cd)
	assert.Nil(t, err)

	t.Run("Trace", func(t *testing.T) {
		c := testChunk(t, model.StreamTrace, testTraceFile)

		traces, res, err := ReadTraces(ctx, c, dict)
		assert.Nil(t, err)
		//fmt.Print(res) // for debug
		//_ = os.WriteFile("test-service.traces.expected.txt", []byte(res), 0644) // for debug
		assert.Equal(t, readTestFile(t, expectedTraceLog), stripLines(res))
		assert.Equal(t, 69, len(traces.List))

		t2 = t2.Add(time.Nanosecond)
		for i, a := range traces.List {
			assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
			assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
		}
	})
}

//\nblock #1. threadId=   1, real time: 1691167327716 - 22:12:07.716 , offset=8 / 8\ncall  [  1: 0] tagId=9|'void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profi
//\nblock #1. threadId=   1, real time: 1691167327716 - 16:42:07.716 , offset=8 / 8\ncall  [  1: 0] tagId=9|'void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profi

func TestReadTrace_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.ERROR)
	t1 := time.Date(2023, 8, 4, 16, 42, 6, 615000000, time.UTC)
	t2 := time.Date(2023, 8, 4, 16, 48, 0, 317000000, time.UTC)

	testDictionaryFile := filepath.Join(ResourceDir, "u5min", "u5min-service.dictionary.protocol")
	testTraceFile := filepath.Join(ResourceDir, "u5min", "u5min-service.traces.0.protocol")
	expectedTraceLog := filepath.Join(ResourceDir, "u5min", "u5min-service.traces.expected.txt")

	cd := testChunk(t, model.StreamDictionary, testDictionaryFile)
	dict, _, err := ReadDictionary(ctx, cd)
	assert.Nil(t, err)

	t.Run("Trace", func(t *testing.T) {
		c := testChunk(t, model.StreamTrace, testTraceFile)

		traces, res, err := ReadTraces(ctx, c, dict)
		assert.Nil(t, err)
		//_ = os.WriteFile("u5min-service.traces.expected.txt", []byte(res), 0644) // for debug

		assert.Equal(t, readTestFile(t, expectedTraceLog), stripLines(res))
		assert.Equal(t, 512, len(traces.List))

		t2 = t2.Add(time.Nanosecond)
		for i, a := range traces.List {
			assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
			assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
		}
	})
}
