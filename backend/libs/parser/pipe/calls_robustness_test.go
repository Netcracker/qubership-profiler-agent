package pipe

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin CallsPipeReader against a torn or corrupt calls stream. A
// phase-4 soak crashed a collector with `index out of range [-1]` at
// threadNames[threadIndex]: a mid-record read failed, ReadVarInt returned -1
// with the error dropped, and the upper-bound-only guard let -1 through. The
// reader must now stop decoding without panicking and never publish a partial
// record. See calls.go.

const callsHeaderMagic = 0xFFFEFDFC

// putCallsHeader writes the version-tagged calls file header — (magic<<32|version)
// then the absolute base_ms, both fixed big-endian longs — matching the header
// the decoder reads in calls.go.
func putCallsHeader(buf *bytes.Buffer, version uint32, baseMs int64) {
	var eight [8]byte
	binary.BigEndian.PutUint64(eight[:], uint64(callsHeaderMagic)<<32|uint64(version))
	buf.Write(eight[:])
	binary.BigEndian.PutUint64(eight[:], uint64(baseMs))
	buf.Write(eight[:])
}

// putZigZag writes v the way the decoder reads a record's start delta
// (ReadVarIntZigZag). Non-negative v encodes as v<<1.
func putZigZag(buf *bytes.Buffer, v int64) {
	putUvarint(buf, uint64((v<<1)^(v>>63)))
}

// putCallsV1RecordHead writes the fixed fields of one version-1 record up to
// (not including) nParams: a zero time delta, small method/duration/calls,
// threadIndex 0 with an inline name, and zeroed counters. Callers append the
// param section to trip a specific limit.
func putCallsV1RecordHead(buf *bytes.Buffer, threadName string) {
	putZigZag(buf, 0)  // time delta
	putUvarint(buf, 7) // method
	putUvarint(buf, 42)
	putUvarint(buf, 1) // child calls
	putUvarint(buf, 0) // threadIndex 0 == len(table) → inline name follows
	buf.Write(encodeVarString(threadName))
	putUvarint(buf, 0) // logsWritten
	putUvarint(buf, 0) // logsGenerated − logsWritten
	putUvarint(buf, 0) // traceFileIndex
	putUvarint(buf, 0) // bufferOffset
	putUvarint(buf, 0) // recordIndex
}

// callItemKey is a stable identity for a decoded record: ordinal, byte offset,
// and the full CSV projection. Two runs that decode the same record produce the
// same key, so a truncated run's output can be checked against the full run's.
func callItemKey(it CallItem) string {
	return fmt.Sprintf("%d|%d|%s", it.id, it.pos, it.Call.Csv())
}

// TestCallsPipeReaderTruncatedNoPanic is the primary regression: it reproduces
// the incident's torn-read space deterministically. It builds a valid
// multi-record stream (one record introduces a new thread name, one carries a
// list param so the param sub-loop and its strings run), then feeds EVERY prefix
// of it to the reader. Each prefix must (a) terminate — proven by a done/timeout
// guard, since a wedged reader would otherwise hang — and (b) emit only complete
// records: its output must be an exact prefix of the full stream's records, so a
// cut inside the last param string yields only the records fully decoded before
// it, never a partial one. Before the fix, the prefix ending just before
// threadIndex panics the reader's goroutine and crashes the test binary.
func TestCallsPipeReaderTruncatedNoPanic(t *testing.T) {
	ctx := context.Background()
	const baseMs int64 = 1_700_000_000_000

	records := []wire.CallRecord{
		{DeltaMs: 5, Method: 7, DurationMs: 42, ChildCalls: 1, ThreadName: "main"},
		{
			DeltaMs: 60_000, Method: 9, DurationMs: 7, ChildCalls: 2, ThreadName: "worker-1",
			Params: map[int][]string{3: {"alpha", "beta", "gamma"}, 5: {"solo"}},
		},
	}
	data := wire.CallsStreamRecords(baseMs, records)

	// Canonical decode of the whole stream.
	var canonical []string
	for it := range CallsPipeReader(ctx, OpenDataAsReader(data, false)) {
		canonical = append(canonical, callItemKey(it))
	}
	require.Len(t, canonical, len(records), "the full stream decodes to every record")

	for n := 0; n <= len(data); n++ {
		prefix := data[:n]

		done := make(chan struct{})
		var got []string
		go func() {
			for it := range CallsPipeReader(ctx, OpenDataAsReader(prefix, false)) {
				got = append(got, callItemKey(it))
			}
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("prefix len %d/%d: reader did not terminate", n, len(data))
		}

		require.LessOrEqualf(t, len(got), len(canonical),
			"prefix len %d emitted more records than the full stream", n)
		for i, g := range got {
			require.Equalf(t, canonical[i], g,
				"prefix len %d record %d is not a complete, unaltered prefix record", n, i)
		}
	}
}

// TestCallsPipeReaderUnknownThreadIndex pins the reachable upper-bound "unknown"
// branch. A valid new-index record writes the thread name right after
// threadIndex, but the unknown path reads no name — so this hand-builds one
// record whose threadIndex (5) is past the empty table, with no inline name, and
// asserts the record decodes with a synthesized name instead of panicking.
func TestCallsPipeReaderUnknownThreadIndex(t *testing.T) {
	ctx := context.Background()

	var buf bytes.Buffer
	putCallsHeader(&buf, 1, 1_600_000_000_000)
	putZigZag(&buf, 0)  // time delta
	putUvarint(&buf, 7) // method
	putUvarint(&buf, 42)
	putUvarint(&buf, 1) // child calls
	putUvarint(&buf, 5) // threadIndex 5 != len(table) 0 → no name, "unknown # 5"
	putUvarint(&buf, 0) // logsWritten
	putUvarint(&buf, 0) // logsGenerated − logsWritten
	putUvarint(&buf, 0) // traceFileIndex
	putUvarint(&buf, 0) // bufferOffset
	putUvarint(&buf, 0) // recordIndex
	putUvarint(&buf, 0) // nParams

	var got []CallItem
	for it := range CallsPipeReader(ctx, OpenDataAsReader(buf.Bytes(), false)) {
		got = append(got, it)
	}

	require.Len(t, got, 1)
	assert.Equal(t, "unknown # 5", got[0].Call.ThreadName)
}

// TestCallsPipeReaderParamLimits pins the three corruption ceilings. Each
// sub-test hand-builds a record that trips exactly one cap and asserts BOTH
// guarantees: no record is emitted, and the reader records the breach in Err()
// (a plain break would look like a clean end and the ingest decoder could not
// tell a malformed stream from EOF).
func TestCallsPipeReaderParamLimits(t *testing.T) {
	ctx := context.Background()
	const baseMs int64 = 1_600_000_000_000

	drain := func(t *testing.T, data []byte) *PipeReader {
		t.Helper()
		r := OpenDataAsReader(data, false)
		n := 0
		for range CallsPipeReader(ctx, r) {
			n++
		}
		assert.Zero(t, n, "no record must be emitted for the corrupt record")
		assert.Error(t, r.Err(), "the cap breach must surface via Err(), not a clean end")
		return r
	}

	t.Run("nParams over cap", func(t *testing.T) {
		var buf bytes.Buffer
		putCallsHeader(&buf, 1, baseMs)
		putCallsV1RecordHead(&buf, "t")
		putUvarint(&buf, uint64(maxCallParams+1)) // nParams past the cap
		drain(t, buf.Bytes())
	})

	t.Run("summed value count over budget", func(t *testing.T) {
		var buf bytes.Buffer
		putCallsHeader(&buf, 1, baseMs)
		putCallsV1RecordHead(&buf, "t")
		putUvarint(&buf, 1)                             // nParams
		putUvarint(&buf, 0)                             // paramId
		putUvarint(&buf, uint64(maxCallRecordValues+1)) // value count past the budget
		drain(t, buf.Bytes())
	})

	t.Run("param bytes over budget", func(t *testing.T) {
		// One var-string whose wire span alone exceeds the byte budget. The
		// length stays under ReadVarString's own code-unit cap so the budget
		// guard — not the string cap — is what fires.
		const codeUnits = maxCallRecordParamBytes/2 + 1 // 2 bytes per code unit
		var buf bytes.Buffer
		putCallsHeader(&buf, 1, baseMs)
		putCallsV1RecordHead(&buf, "t")
		putUvarint(&buf, 1)                 // nParams
		putUvarint(&buf, 0)                 // paramId
		putUvarint(&buf, 1)                 // one value
		putUvarint(&buf, uint64(codeUnits)) // var-string length
		buf.Write(make([]byte, 2*codeUnits))
		drain(t, buf.Bytes())
	})
}
