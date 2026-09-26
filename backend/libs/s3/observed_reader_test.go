package s3

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedReaderAt answers each ReadAt with the next scripted result and
// repeats the last one once the script runs out, as a minio object replays
// its first error.
type scriptedReaderAt struct {
	results []readResult
	calls   int
}

type readResult struct {
	n   int
	err error
}

func (r *scriptedReaderAt) ReadAt(_ []byte, _ int64) (int, error) {
	i := min(r.calls, len(r.results)-1)
	r.calls++
	return r.results[i].n, r.results[i].err
}

const observedSize = 100

// readGets performs one ReadAt at each offset and returns the change of the
// "get" series across them.
func readGets(t *testing.T, ctx context.Context, fake *scriptedReaderAt, offsets ...int64) opCounts {
	t.Helper()
	reg := newTestRegistry()
	r := NewObservedReaderAt(ctx, fake, observedSize)
	before := readCounts(t, reg, OperationGet)
	for _, off := range offsets {
		_, _ = r.ReadAt(make([]byte, 10), off)
	}
	return readCounts(t, reg, OperationGet).minus(before)
}

func TestObservedReaderAt_SuccessfulReadsCountRequestsNotObjects(t *testing.T) {
	fake := &scriptedReaderAt{results: []readResult{{10, nil}}}
	assert.Equal(t, opCounts{requests: 3}, readGets(t, context.Background(), fake, 0, 10, 20))
}

// A read of the last byte is inside the object and reaches S3, and it ends
// in io.EOF.
func TestObservedReaderAt_ReadEndingInEOFIsASuccess(t *testing.T) {
	fake := &scriptedReaderAt{results: []readResult{{1, io.EOF}}}
	assert.Equal(t, opCounts{requests: 1}, readGets(t, context.Background(), fake, observedSize-1))
}

func TestObservedReaderAt_OffsetOutsideTheObjectRecordsNothing(t *testing.T) {
	for _, c := range []struct {
		name string
		off  int64
	}{
		{"negative", -1},
		{"at size", observedSize},
	} {
		t.Run(c.name, func(t *testing.T) {
			fake := &scriptedReaderAt{results: []readResult{{0, io.EOF}}}
			assert.Equal(t, opCounts{}, readGets(t, context.Background(), fake, c.off))
			assert.Equal(t, 1, fake.calls, "ReadAt(_, %d) is still delegated", c.off)
		})
	}
}

// The minio client returns the first error again from every later read of
// the object without a request, so only the first counts.
func TestObservedReaderAt_FailureCountsOncePerObject(t *testing.T) {
	fake := &scriptedReaderAt{results: []readResult{{0, errors.New("503 slow down")}}}
	assert.Equal(t, opCounts{errors: 1}, readGets(t, context.Background(), fake, 0, 10, 20))
	assert.Equal(t, 3, fake.calls)
}

func TestObservedReaderAt_FailureUnderCancelledCallerRecordsNothing(t *testing.T) {
	fake := &scriptedReaderAt{results: []readResult{{0, context.Canceled}}}
	assert.Equal(t, opCounts{}, readGets(t, cancelledContext(), fake, 0))
}

func TestObservedReaderAt_ReadAfterCloseRecordsNothing(t *testing.T) {
	reg := newTestRegistry()
	fake := &scriptedReaderAt{results: []readResult{{10, nil}}}
	r := NewObservedReaderAt(context.Background(), fake, observedSize)
	before := readCounts(t, reg, OperationGet)
	r.Close()
	n, err := r.ReadAt(make([]byte, 10), 0)
	require.NoError(t, err)
	assert.Equal(t, 10, n, "ReadAt after Close is still delegated")
	assert.Equal(t, opCounts{}, readCounts(t, reg, OperationGet).minus(before))
}
