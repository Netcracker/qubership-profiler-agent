package pipe

import (
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallsTimeAccumulation guards the calls-time reconstruction. The wire stores
// each record's start as a zig-zag delta from the previous record; the decoder
// must accumulate from the file header (01-write-contract.md §5.1). The pre-fix
// decoder read each delta as an absolute offset from the header, so it was correct
// only for the first record and drifted on every later one.
func TestCallsTimeAccumulation(t *testing.T) {
	const baseMs int64 = 1_700_000_000_000
	deltas := []int64{5, 60_000, 120_000} // 5 ms, then one and two minutes apart
	data := wire.CallsStream(baseMs, deltas)

	pipe := CallsPipeReader(context.Background(), OpenDataAsReader(data, false))

	var got []int64
	for item := range pipe {
		got = append(got, item.Time.UnixMilli())
	}

	want := []int64{
		baseMs + 5,                    // base + delta[0]
		baseMs + 5 + 60_000,           // + delta[1]
		baseMs + 5 + 60_000 + 120_000, // + delta[2]
	}
	require.Equal(t, len(want), len(got), "record count")
	assert.Equal(t, want, got)
}
