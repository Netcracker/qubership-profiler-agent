package hotstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseValueRef(t *testing.T) {
	ref, err := ParseValueRef("sql:3:1024")
	require.NoError(t, err)
	assert.Equal(t, ValueRef{StreamSql, 3, 1024}, ref)
	assert.Equal(t, "sql:3:1024", ref.String())

	for _, bad := range []string{"", "sql:3", "trace:1:0", "sql:x:0", "sql:1:x", "sql:-1:-2"} {
		_, err := ParseValueRef(bad)
		assert.Error(t, err, bad)
	}
}

// TestBigValues pins the value-segment read path the seal pass and the
// internal values endpoint share: var-strings located by (stream, seq,
// offset), duplicate PARAM_BIG_DEDUP references, and the degrade-not-fail
// behaviour for missing segments and torn references.
func TestBigValues(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	sqlData, sqlOffs := wire.ValueStream([]string{"SELECT 1", "SELECT * FROM calls WHERE ts > ?"})
	xmlData, xmlOffs := wire.ValueStream([]string{"<payload attr=\"v\"/>"})

	sqlSeg, err := pr.OpenSegment(StreamSql, 1)
	require.NoError(t, err)
	_, err = sqlSeg.Write(sqlData)
	require.NoError(t, err)
	xmlSeg, err := pr.OpenSegment(StreamXml, 1)
	require.NoError(t, err)
	_, err = xmlSeg.Write(xmlData)
	require.NoError(t, err)

	refs := []ValueRef{
		{StreamSql, 1, sqlOffs[0]},
		{StreamSql, 1, sqlOffs[1]},
		{StreamSql, 1, sqlOffs[0]}, // a dedup tag repeats its reference
		{StreamXml, 1, xmlOffs[0]},
		{StreamSql, 7, 0},               // segment that never existed
		{StreamXml, 1, xmlOffs[0] + 99}, // offset past the stream: torn
	}
	// BigValues flushes the live gzip writers itself.
	values, err := store.BigValues(ctx, key, refs)
	require.NoError(t, err)

	assert.Equal(t, map[ValueRef]string{
		{StreamSql, 1, sqlOffs[0]}: "SELECT 1",
		{StreamSql, 1, sqlOffs[1]}: "SELECT * FROM calls WHERE ts > ?",
		{StreamXml, 1, xmlOffs[0]}: "<payload attr=\"v\"/>",
	}, values, "resolvable references resolve; missing and torn ones drop out")

	_, err = store.BigValues(ctx, PodRestartKey{Namespace: "no", Service: "such", PodName: "pod", RestartTimeMs: 1}, refs)
	assert.Error(t, err, "an unknown pod-restart is the caller's bug, not a degrade")
}

// TestResolveBigValuesAggregatesLoss pins PR 708 review #5: when one evicted
// value segment starves many calls, the seal pass logs a single summary of the
// blast radius, not one "lost … to eviction" line per call.
func TestResolveBigValuesAggregatesLoss(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.WARNING)
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	sqlData, sqlOffs := wire.ValueStream([]string{"SELECT 1"})
	sqlSeg, err := pr.OpenSegment(StreamSql, 1)
	require.NoError(t, err)
	_, err = sqlSeg.Write(sqlData)
	require.NoError(t, err)
	require.NoError(t, pr.FlushSegments())

	good := ValueRef{StreamSql, 1, sqlOffs[0]}
	gone := ValueRef{StreamSql, 7, 0} // a segment that never existed / was evicted

	// Five calls all reference the same missing segment; one resolves cleanly.
	rows := make([]sealRow, 0, 6)
	for i := 0; i < 5; i++ {
		rows = append(rows, sealRow{idx: CallIndexRow{RecordIndex: i}, bigRefs: []ValueRef{gone}})
	}
	rows = append(rows, sealRow{idx: CallIndexRow{RecordIndex: 99}, bigRefs: []ValueRef{good}})

	out := captureStdout(t, func() { store.resolveBigValues(ctx, pr, rows) })

	for i := 0; i < 5; i++ {
		assert.Equal(t, TruncDiskBudget, rows[i].truncated, "a call whose value segment is gone seals truncated")
	}
	assert.Empty(t, rows[5].truncated, "the resolvable call is untouched")
	assert.Equal(t, map[string]string{good.String(): "SELECT 1"}, rows[5].bigValues)

	assert.NotContains(t, out, "to eviction", "the per-call eviction line is gone")
	assert.Equal(t, 1, strings.Count(out, "sealed 5 call(s) truncated"),
		"exactly one aggregated summary for the whole pass, not one line per call")
	assert.Contains(t, out, "across 1 segment(s)", "the summary reports the blast radius")
}

// captureStdout redirects os.Stdout for the duration of fn and returns what was
// written. The log package prints straight to stdout, so this is the only seam
// to assert on its output.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	require.NoError(t, w.Close())
	os.Stdout = orig
	return <-done
}
