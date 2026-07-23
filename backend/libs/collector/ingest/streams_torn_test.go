package ingest

// reports2#3: a torn or corrupt calls, params, or suspend stream must fail the
// decoder so the server answers ACK_ERROR and the agent resends, instead of
// ACK_OK silently accepting the truncated data. Before the fix only the
// dictionary decoder propagated the reader's terminal error; these three
// returned nil after their loop, so a tear looked identical to a clean EOF.

import (
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decoderErrorsFor registers one stream on a fresh listener, feeds it payload,
// then disconnects the pod so the decoder drains to the stream's end. It returns
// DecoderErrors, which startDecoder bumps once when a decoder rejects its stream
// as malformed. PodDisconnected closes the pipe (the decoder's EOF) and waits
// for the decode goroutine, so the counter has settled by the time it returns.
func decoderErrorsFor(t *testing.T, streamType string, payload []byte) uint64 {
	t.Helper()
	ctx := context.Background()
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	l := NewListener(store)
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))

	handle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, handle, streamType, 0, 0, 0, 0, 0))

	// A torn stream fails the decoder, and the resulting CloseWithError makes a
	// later write fail; the write result is irrelevant here — the tear is
	// observed through the counter, so ignore it.
	_, _ = l.AppendData(ctx, pod, handle, string(payload))
	l.PodDisconnected(ctx, pod)
	return l.IngestStatsSnapshot().DecoderErrors
}

// TestCallsDecoderSurfacesTornStream pins the reports2#3 fix for calls: a stream
// torn inside a multi-byte field surfaces the reader's error instead of passing
// as a clean EOF.
func TestCallsDecoderSurfacesTornStream(t *testing.T) {
	full := wire.CallsStream(1_700_000_000_000, []int64{0, 5})

	assert.Zero(t, decoderErrorsFor(t, model.StreamCalls, full),
		"a well-formed calls stream must reach a clean EOF and be accepted")

	// Cut inside the 8-byte base_ms header long (8 magic bytes + 4 of 8 base
	// bytes): ReadFixedLong gets a partial read (io.ErrUnexpectedEOF), which the
	// reader records as a torn stream rather than a clean end.
	assert.EqualValues(t, 1, decoderErrorsFor(t, model.StreamCalls, full[:12]),
		"a calls stream torn mid-field must fail the decoder, not be accepted as EOF")
}

// TestParamsDecoderSurfacesTornStream pins the reports2#3 fix for params: a
// complete first phrase followed by a torn next-phrase length prefix fails the
// decoder, while the clean phrase alone is accepted.
func TestParamsDecoderSurfacesTornStream(t *testing.T) {
	clean := wire.ParamsStream([]wire.ParamDef{
		{Name: "userId", IsIndex: true, Order: 0, Signature: "S"},
	})

	assert.Zero(t, decoderErrorsFor(t, model.StreamParams, clean),
		"a well-formed params stream must reach a clean EOF and be accepted")

	// A valid phrase, then 2 of the 4 fixed-int bytes of the next phrase's length
	// prefix: the decoder consumes the first phrase, then tears on ReadFixedInt.
	torn := append(append([]byte{}, clean...), 0x00, 0x01)
	assert.EqualValues(t, 1, decoderErrorsFor(t, model.StreamParams, torn),
		"a params stream torn mid-field must fail the decoder, not be accepted as EOF")
}

// TestSuspendDecoderSurfacesTornStream pins the reports2#3 fix for suspend, with
// the same valid-phrase-then-torn-prefix shape as the params case.
func TestSuspendDecoderSurfacesTornStream(t *testing.T) {
	clean := wire.SuspendStream(1_700_000_000_000, []wire.SuspendEvent{
		{DeltaMs: 10, AmountMs: 3},
	})

	assert.Zero(t, decoderErrorsFor(t, model.StreamSuspend, clean),
		"a well-formed suspend stream must reach a clean EOF and be accepted")

	torn := append(append([]byte{}, clean...), 0x00, 0x01)
	assert.EqualValues(t, 1, decoderErrorsFor(t, model.StreamSuspend, torn),
		"a suspend stream torn mid-field must fail the decoder, not be accepted as EOF")
}
