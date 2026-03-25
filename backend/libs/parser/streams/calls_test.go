package streams

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestReadCalls_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.TRACE)
	t1 := time.Date(2023, 7, 24, 12, 25, 0, 0, time.UTC)
	t2 := time.Date(2023, 7, 24, 12, 29, 0, 0, time.UTC)

	testCallsFile := filepath.Join(ResourceDir, "test-service", "test-service.calls.0.protocol")
	expectedCsv := filepath.Join(ResourceDir, "test-service", "test-service.calls.expected.txt")
	t.Run("Calls", func(t *testing.T) {
		c := testChunk(t, model.StreamCalls, testCallsFile)

		logged := log.CaptureAsString(func() {
			Calls, csv, err := ReadCalls(ctx, c)
			assert.Nil(t, err)
			assert.Equal(t, 45, len(Calls.List))
			assert.Equal(t, 14, len(Calls.RequiredIds))

			t2 = t2.Add(time.Nanosecond)
			for i, a := range Calls.List {
				assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
				assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
			}

			assert.Equal(t, readTestFile(t, expectedCsv), stripLines(csv))

		})
		assert.Equal(t, stripLines(`
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * reading 'calls': Chunk[calls, 0] (00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00, 28874 bytes)
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * file format: 4
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * start time: 1690201585956 -  2023-07-24 12:26:25.956 +0000 UTC
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * read 'calls': EOF. 45 calls, 28874 model bytes
`), stripLines(logged))
	})
}

func TestReadCalls_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t1 := time.Date(2023, 8, 4, 16, 41, 0, 0, time.UTC)
	t2 := time.Date(2023, 8, 4, 16, 43, 0, 0, time.UTC)

	testCallsFile := filepath.Join(ResourceDir, "u5min", "u5min-service.calls.0.protocol")
	expectedCsv := filepath.Join(ResourceDir, "u5min", "u5min-service.calls.expected.txt")

	t.Run("Calls", func(t *testing.T) {
		c := testChunk(t, model.StreamCalls, testCallsFile)

		logged := log.CaptureAsString(func() {
			Calls, csv, err := ReadCalls(ctx, c)
			assert.Nil(t, err)
			//fmt.Print(res)
			assert.Equal(t, 422, len(Calls.List))
			assert.Equal(t, 14, len(Calls.RequiredIds))
			assert.Equal(t, []data.TagId{7, 9, 19, 41, 45, 78, 84, 119, 173, 174, 299, 555, 593, 769}, Calls.Tags())

			t2 = t2.Add(time.Nanosecond)
			for i, a := range Calls.List {
				assert.True(t, t1.Before(a.Time), "row %d, invalid start %v", i, a.Time)
				assert.True(t, t2.After(a.Time), "row %d, invalid end %v", i, a.Time)
			}

			assert.Equal(t, readTestFile(t, expectedCsv), stripLines(csv))
		})
		assert.Equal(t, stripLines(`
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * reading 'calls': Chunk[calls, 0] (00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00, 309008 bytes)
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * file format: 4
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * start time: 1691167328395 -  2023-08-04 16:42:08.395 +0000 UTC
[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=streams/calls.go:12]  * read 'calls': EOF. 422 calls, 309008 model bytes
`), stripLines(logged))

	})
}
