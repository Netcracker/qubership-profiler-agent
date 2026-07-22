package integration

// The regression that would have caught the pre-migration bug:
// xitongsys/parquet-go aligned a file's footer schema to the target struct by
// column INDEX, so reading an older, narrower file through a wider CallV2
// panicked instead of null-filling. The parquet-go/parquet-go reader matches
// columns by NAME: additive evolution (and column removal) is backward-
// readable, a missing column reads back as zero/NULL, and only a non-additive
// change (rename, type change) needs the profiler.schema_version footer stamp.

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// callV1Shaped is CallV2 as the seal pass wrote it BEFORE slice 6 added
// big_params_json — the file shape that made the old reader panic. The tags
// mirror the CallV2 originals because the column NAME is the compatibility
// contract.
type callV1Shaped struct {
	TsMs           int64  `parquet:"ts_ms"`
	PodId          string `parquet:"pod_id,dict"`
	RestartTimeMs  int64  `parquet:"restart_time_ms"`
	TraceFileIndex int32  `parquet:"trace_file_index"`
	BufferOffset   int32  `parquet:"buffer_offset"`
	RecordIndex    int32  `parquet:"record_index"`
	ThreadName     string `parquet:"thread_name,dict"`

	Namespace   string `parquet:"namespace,dict"`
	ServiceName string `parquet:"service_name,dict"`
	PodName     string `parquet:"pod_name,dict"`
	Method      string `parquet:"method,dict"`

	DurationMs    int32 `parquet:"duration_ms"`
	CpuTimeMs     int64 `parquet:"cpu_time_ms"`
	WaitTimeMs    int64 `parquet:"wait_time_ms"`
	MemoryUsed    int64 `parquet:"memory_used"`
	QueueWaitMs   int32 `parquet:"queue_wait_ms"`
	SuspendMs     int32 `parquet:"suspend_ms"`
	ChildCalls    int32 `parquet:"child_calls"`
	Transactions  int32 `parquet:"transactions"`
	LogsGenerated int64 `parquet:"logs_generated"`
	LogsWritten   int64 `parquet:"logs_written"`
	FileRead      int64 `parquet:"file_read"`
	FileWritten   int64 `parquet:"file_written"`
	NetRead       int64 `parquet:"net_read"`
	NetWritten    int64 `parquet:"net_written"`

	ErrorFlag      bool   `parquet:"error_flag"`
	RetentionClass string `parquet:"retention_class,dict"`

	Params          storageparquet.Parameters `parquet:"params" parquet-value:",list"`
	TraceBlob       []byte                    `parquet:"trace_blob,optional"`
	TruncatedReason *string                   `parquet:"truncated_reason,optional,dict"`
}

// callMinimalShaped holds only the first few CallV2 columns — the shape of a
// hypothetical much older file, pinning that ANY number of missing trailing
// columns null-fills (the old reader's index alignment broke on the very
// first missing one).
type callMinimalShaped struct {
	TsMs   int64  `parquet:"ts_ms"`
	PodId  string `parquet:"pod_id,dict"`
	Method string `parquet:"method,dict"`
}

// TestNarrowFileReadsThroughCurrentCallV2 seals nothing: it plants a
// v1-shaped object in the in-test S3 and drives the two production cold
// readers over it. Both must null-fill the missing big_params_json column
// instead of panicking.
func TestNarrowFileReadsThroughCurrentCallV2(t *testing.T) {
	ctx := context.Background()
	fake := newColdFakeStore()

	tuple := model.PodTuple{
		Namespace: hotstoreNs, Service: hotstoreSvc, Pod: "pod-v1-shaped", RestartTimeMs: baseMs - 1000,
	}
	blob := append(make([]byte, 8), "the sealed blob"...)
	row := callV1Shaped{
		TsMs:           baseMs + 5,
		PodId:          tuple.Namespace + "/" + tuple.Service + "/" + tuple.Pod,
		RestartTimeMs:  tuple.RestartTimeMs,
		TraceFileIndex: 1,
		BufferOffset:   8,
		RecordIndex:    0,
		ThreadName:     "exec-1",
		Namespace:      tuple.Namespace,
		ServiceName:    tuple.Service,
		PodName:        tuple.Pod,
		Method:         "com.example.Service.handle",
		DurationMs:     10,
		SuspendMs:      3,
		RetentionClass: "short_clean",
		Params:         storageparquet.Parameters{"request.id": {"req-1"}},
		TraceBlob:      blob,
	}

	// A v1 writer: same codec, no schema-version stamp yet.
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[callV1Shaped](&buf, parquet.Compression(&parquet.Zstd))
	_, err := w.Write([]callV1Shaped{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	const key = "parquet/v1/short_clean/2023/11/14/22/collector-0-v1shaped.parquet"
	require.NoError(t, fake.PutBytes(ctx, key, buf.Bytes()))
	ref := cold.FileRef{Key: key, Size: int64(buf.Len()), Class: "short_clean", Hash: model.PodRestartHash(tuple)}

	t.Run("list path null-fills through the projection", func(t *testing.T) {
		rows, err := cold.ScanFile(ctx, fake, ref, model.CallsQuery{
			FromMs: baseMs, ToMs: baseMs + 60_000,
		}, nil)
		require.NoError(t, err, "a narrower file must scan, not fail")
		require.Len(t, rows, 1)
		assert.Equal(t, "com.example.Service.handle", rows[0].Method)
		assert.Equal(t, map[string][]string{"request.id": {"req-1"}}, rows[0].Params)
		assert.Equal(t, int64(baseMs+5), rows[0].TsMs)
		assert.Empty(t, rows[0].TruncatedReason)
	})

	t.Run("point fetch null-fills the missing column", func(t *testing.T) {
		pk := model.PK{
			PodNamespace: tuple.Namespace, PodService: tuple.Service, PodName: tuple.Pod,
			RestartTimeMs: tuple.RestartTimeMs, TraceFileIndex: 1, BufferOffset: 8, RecordIndex: 0,
		}
		src := &cold.Source{Store: fake}
		got, ok, err := src.FetchCall(ctx, []cold.FileRef{ref}, pk, nil, cold.TreeColumns)
		require.NoError(t, err, "reading a v1-shaped file through the PK scan must not panic")
		require.True(t, ok)
		assert.Equal(t, blob, got.TraceBlob, "columns present in the file round-trip")
		assert.Nil(t, got.BigParamsJson, "the column added after the file was written reads as NULL")
		assert.Nil(t, got.TruncatedReason)
	})

	t.Run("a minimal ancient file zero-fills every missing column", func(t *testing.T) {
		var mini bytes.Buffer
		w := parquet.NewGenericWriter[callMinimalShaped](&mini, parquet.Compression(&parquet.Zstd))
		_, err := w.Write([]callMinimalShaped{{TsMs: baseMs + 7, PodId: "ns/svc/pod", Method: "m"}})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		f, err := parquet.OpenFile(bytes.NewReader(mini.Bytes()), int64(mini.Len()))
		require.NoError(t, err)
		r := parquet.NewGenericReader[storageparquet.CallV2](f)
		defer func() { _ = r.Close() }()
		rows := make([]storageparquet.CallV2, r.NumRows())
		n, err := r.Read(rows)
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
		}
		require.Equal(t, 1, n)
		assert.Equal(t, int64(baseMs+7), rows[0].TsMs)
		assert.Equal(t, "m", rows[0].Method)
		assert.Zero(t, rows[0].DurationMs)
		assert.False(t, rows[0].ErrorFlag)
		assert.Empty(t, rows[0].Params)
		assert.Nil(t, rows[0].TraceBlob)
		assert.Nil(t, rows[0].TruncatedReason)
		assert.Nil(t, rows[0].BigParamsJson)
	})
}
