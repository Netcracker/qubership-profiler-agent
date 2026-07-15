package hotstore

import (
	"context"
	"testing"

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
