package streams

import (
	"bytes"
	"context"
	"testing"

	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallsTimeAccumulation guards the calls-time reconstruction in the streams
// decoder, the twin of the pipe decoder (01-write-contract.md §5.1). The wire
// stores each record's start as a zig-zag delta from the previous record; before
// the fix the decoder read each delta as an absolute offset from the file header,
// correct only for the first record.
func TestCallsTimeAccumulation(t *testing.T) {
	const baseMs int64 = 1_700_000_000_000
	deltas := []int64{5, 60_000, 120_000}
	data := wire.CallsStream(baseMs, deltas)

	c := model.NewChunk(uuid0, model.StreamCalls, 0, 0, 0)
	c.Init(bytes.NewBuffer(data))

	parsed, _, err := ReadCalls(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, len(deltas), len(parsed.List))

	want := []int64{
		baseMs + 5,
		baseMs + 5 + 60_000,
		baseMs + 5 + 60_000 + 120_000,
	}
	got := make([]int64, 0, len(parsed.List))
	for _, ci := range parsed.List {
		got = append(got, ci.Time.UnixMilli())
	}
	assert.Equal(t, want, got)
}
