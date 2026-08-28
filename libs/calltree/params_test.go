package calltree

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLSignature(t *testing.T) {
	for sql, want := range map[string]string{
		"SELECT * FROM orders WHERE id = 123": "S*FoWi=",
		"SELECT * FROM orders WHERE id = 4":   "S*FoWi=",
		"WHERE name = 'John''s', age = 7":     "Wn=a=",  // literals (with '' escapes), commas, and digits vanish
		"UPDATE t1 SET x2 = ?":                "UtSx=?", // digits vanish without splitting their word
		"SELECT ab1cd FROM t":                 "SaFt",
		"":                                    "",
	} {
		assert.Equal(t, want, sqlSignature(sql), "signature of %q", sql)
	}
}

// TestBuildSQLGroupingAndBinds pins the R11 mini-tree (02 §2.5.3): SQL texts
// differing only in literals share a signature and fold into one group whose
// value is the first-seen full text; each invocation's binds nest under its
// own SQL's group.
func TestBuildSQLGroupingAndBinds(t *testing.T) {
	sqls := map[int64]string{
		1: "SELECT * FROM orders WHERE id = 1",
		2: "SELECT * FROM orders WHERE id = 42",
		3: "DELETE FROM orders",
	}
	opts := dictOpt()
	opts.BigValue = func(stream string, seq int, offset int64) (string, bool) {
		v, ok := sqls[offset]
		return v, ok && stream == "sql"
	}
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(1, 2), wire.BigTag(0, 5, true, 1, 1), wire.Tag(0, 7, "1"), wire.Exit(5),
		wire.Enter(1, 2), wire.BigTag(0, 5, true, 1, 2), wire.Tag(0, 7, "42"), wire.Exit(7),
		wire.Enter(1, 2), wire.BigTag(0, 5, true, 1, 3), wire.Exit(3),
		wire.Exit(3),
	}})

	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)
	q := tree.Root.Children[0]
	require.Len(t, q.Params, 1, "binds live under their SQL groups, not as a top-level param")

	sql := q.Params[0]
	assert.Equal(t, "sql", tree.Params[sql.ParamIdx])
	require.Len(t, sql.Groups, 2)

	sel := sql.Groups[0]
	assert.Equal(t, "SELECT * FROM orders WHERE id = 1", sel.Value, "the first-seen text represents the group")
	assert.Equal(t, int64(12), sel.DurationMs, "5 + 7 across the invocations sharing the signature")
	assert.Equal(t, int64(2), sel.Executions)
	require.Len(t, sel.Params, 1)
	binds := sel.Params[0]
	assert.Equal(t, "binds", tree.Params[binds.ParamIdx])
	require.Len(t, binds.Groups, 1,
		"binds group by the SQL signature too (signatures.binds = signatures.sql): purely numeric binds share one group")
	assert.Equal(t, ParamGroup{Value: "1", DurationMs: 12, Executions: 2}, binds.Groups[0])

	del := sql.Groups[1]
	assert.Equal(t, "DELETE FROM orders", del.Value)
	assert.Equal(t, int64(3), del.DurationMs)
	assert.Empty(t, del.Params, "no binds rode that invocation")
}

// TestBuildBindsWithoutSQL pins the fallback: a binds value with no preceding
// SQL in its invocation stays a top-level param.
func TestBuildBindsWithoutSQL(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1), wire.Tag(0, 7, "orphan"), wire.Exit(2),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)
	require.Len(t, tree.Root.Params, 1)
	assert.Equal(t, "binds", tree.Params[tree.Root.Params[0].ParamIdx])
	assert.Equal(t, "orphan", tree.Root.Params[0].Groups[0].Value)
}

// TestBuildParamTopNOther drives the Java Hotspot.addTag eviction semantics
// with a cap of 3: as bigger groups arrive, the current smallest is evicted
// into ::other; a newcomer smaller than every live group folds straight in.
func TestBuildParamTopNOther(t *testing.T) {
	events := []wire.TraceEvent{wire.Enter(0, 1)}
	for i := 0; i < 10; i++ {
		// Invocation i carries the unique value v<i> and lasts i+1 ms.
		events = append(events,
			wire.Enter(1, 2), wire.Tag(0, 4, fmt.Sprintf("v%d", i)), wire.Exit(i+1))
	}
	events = append(events, wire.Exit(1))
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: events})

	opts := dictOpt()
	opts.MaxParamGroups = 3
	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)

	q := tree.Root.Children[0]
	require.Len(t, q.Params, 1)
	groups := q.Params[0].Groups
	require.Len(t, groups, 4, "3 live groups plus ::other")
	assert.Equal(t, ParamGroup{Value: "v9", DurationMs: 10, Executions: 1}, groups[0])
	assert.Equal(t, ParamGroup{Value: "v8", DurationMs: 9, Executions: 1}, groups[1])
	assert.Equal(t, ParamGroup{Value: "v7", DurationMs: 8, Executions: 1}, groups[2])
	assert.Equal(t, ParamGroup{Value: OtherGroupValue, DurationMs: 28, Executions: 7}, groups[3],
		"v0..v6 fold into ::other as they are evicted, 1+2+...+7 ms")
}

// TestBuildThousandsOfSQL is the R11 acceptance fixture: a node with 2000
// distinct-signature SQL texts plus three hot ones keeps the default 256
// groups, folds the rest into ::other, and the hot groups — with their nested
// binds — rank on top.
func TestBuildThousandsOfSQL(t *testing.T) {
	hot := map[int64]string{20001: "XHOT", 20002: "YHOT", 20003: "ZHOT"}
	opts := dictOpt()
	opts.BigValue = func(stream string, seq int, offset int64) (string, bool) {
		if v, ok := hot[offset]; ok {
			return v, true
		}
		// Offset i yields "SELECT a a ... a" with i+1 trailing words — a
		// distinct signature per offset.
		return "SELECT" + strings.Repeat(" a", int(offset)+1), true
	}

	events := []wire.TraceEvent{wire.Enter(0, 1)}
	for i := 0; i < 2000; i++ {
		events = append(events, wire.Enter(1, 2), wire.BigTag(0, 5, true, 1, int(i)), wire.Exit(1))
	}
	for _, offset := range []int{20001, 20002, 20003} {
		binds := fmt.Sprintf("b%d", offset)
		for range 2 {
			events = append(events,
				wire.Enter(1, 2), wire.BigTag(0, 5, true, 1, offset), wire.Tag(0, 7, binds), wire.Exit(10))
		}
	}
	events = append(events, wire.Exit(1))
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: events})

	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)

	q := tree.Root.Children[0]
	assert.Equal(t, int64(2006), q.SelfExecutions)
	require.Len(t, q.Params, 1)
	groups := q.Params[0].Groups
	require.Len(t, groups, DefaultMaxParamGroups+1, "256 live groups plus ::other")

	for i, want := range []string{"XHOT", "YHOT", "ZHOT"} {
		assert.Equal(t, want, groups[i].Value, "the hot groups rank on top")
		assert.Equal(t, int64(20), groups[i].DurationMs)
		assert.Equal(t, int64(2), groups[i].Executions)
		require.Len(t, groups[i].Params, 1, "the hot SQL keeps its nested binds")
		assert.Equal(t, ParamGroup{
			Value: fmt.Sprintf("b%d", 20001+i), DurationMs: 20, Executions: 2,
		}, groups[i].Params[0].Groups[0])
	}

	other := groups[len(groups)-1]
	assert.Equal(t, OtherGroupValue, other.Value, "::other is last")
	assert.Equal(t, int64(1747), other.Executions,
		"1744 one-ms newcomers fold straight in, 3 more evicted by the hot groups")
	assert.Equal(t, int64(1747), other.DurationMs)
	assert.Empty(t, other.Params, "::other keeps totals only")
}
