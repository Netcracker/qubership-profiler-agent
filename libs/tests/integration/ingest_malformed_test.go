package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngestGzipChannelIsRefused pins №21: a stream whose bytes open with the
// gzip magic (0x1F 0x8B) is a gzip-wrapped channel the demuxer cannot parse.
// The collector must fail it loudly (ACK_ERROR_MAGIC, so the agent resends),
// not parse compressed garbage, and stay up for the next connection.
func TestIngestGzipChannelIsRefused(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.INFO)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	svc := startCollector(t, ctx, t.TempDir())

	ac := connectAgent(t, ctx)
	handle, err := ac.CommandInitStream(model.StreamDictionary, 0, false)
	require.NoError(t, err)
	require.NotEqual(t, [16]byte{}, handle.ToBin())

	// A gzip member header where the agent's plain dictionary bytes belong.
	require.NoError(t, ac.CommandRcvData(model.StreamDictionary, handle, []byte{0x1F, 0x8B, 0x08, 0x00}))
	_ = ac.Flush()
	ackErr := ac.WaitForAcks()
	assert.Error(t, ackErr, "a gzip-wrapped stream must be refused with ACK_ERROR_MAGIC")
	_ = ac.Close()

	// The decoder-error counter recorded the rejection, and the collector still
	// serves a fresh connection.
	require.Eventually(t, func() bool { return svc.Ingest().IngestStatsSnapshot().DecoderErrors >= 1 },
		2*time.Second, 20*time.Millisecond, "the gzip rejection must bump decoder_errors_total")

	ac2 := connectAgent(t, ctx)
	h2, err := ac2.CommandInitStream(model.StreamDictionary, 0, false)
	require.NoError(t, err, "the collector must survive and accept a new stream")
	require.NoError(t, ac2.CommandRcvData(model.StreamDictionary, h2, []byte("word")))
	require.NoError(t, ac2.Flush())
	require.NoError(t, ac2.WaitForAcks())
	_ = ac2.Close()
}

// TestIngestBytesCounter checks the №21 bytes_total counter reflects the
// RCV_DATA payloads routed into a stream.
func TestIngestBytesCounter(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.INFO)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	svc := startCollector(t, ctx, t.TempDir())

	ac := connectAgent(t, ctx)
	handle, err := ac.CommandInitStream(model.StreamDictionary, 0, false)
	require.NoError(t, err)
	require.NoError(t, ac.CommandRcvData(model.StreamDictionary, handle, []byte("hello-world")))
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	_ = ac.Close()

	require.Eventually(t, func() bool {
		stats := svc.Ingest().IngestStatsSnapshot()
		return stats.BytesRead >= uint64(len("hello-world")) && stats.CommandsReceived > 0
	}, 2*time.Second, 20*time.Millisecond, "ingest counters must record bytes and commands")
}
