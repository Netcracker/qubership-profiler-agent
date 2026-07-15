package calltree

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const timerStartMs = int64(1_700_000_000_000)

// testDict resolves the ids the fixtures below use.
var testDict = map[int]string{
	1: "com.example.Service.handle",
	2: "com.example.Service.query",
	3: "com.example.Service.render",
	4: "request.id",
	5: "sql",
	6: "xml",
}

func dictOpt() Options {
	return Options{Dict: func(id int) (string, bool) {
		w, ok := testDict[id]
		return w, ok
	}}
}

// blobOf renders a per-call blob the way the seal assembler frames it: the
// 8-byte timer epoch, then full chunks of one thread. wire.TraceStream writes
// exactly that shape.
func blobOf(t *testing.T, chunks ...wire.TraceChunk) []byte {
	t.Helper()
	data, _ := wire.TraceStream(timerStartMs, chunks)
	return data
}

func TestBuildNestingAndTimes(t *testing.T) {
	// handle(enter at +5) { query(+7..+11) render(+12..+13) } exit at +20.
	// Deltas accumulate from the timer epoch within the chunk (01 §4.2).
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs + 5, Events: []wire.TraceEvent{
		wire.Enter(5, 1),
		wire.Enter(2, 2), wire.Exit(4),
		wire.Enter(1, 3), wire.Exit(1),
		wire.Exit(7),
	}})

	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)

	root := tree.Root
	require.NotNil(t, root)
	assert.Equal(t, "com.example.Service.handle", tree.Methods[root.MethodIdx])
	assert.Equal(t, int64(0), root.EnterMsRel)
	assert.Equal(t, int64(15), root.DurationMs, "exit at +20 minus enter at +5")
	require.Len(t, root.Children, 2)

	q, r := root.Children[0], root.Children[1]
	assert.Equal(t, "com.example.Service.query", tree.Methods[q.MethodIdx])
	assert.Equal(t, int64(2), q.EnterMsRel)
	assert.Equal(t, int64(4), q.DurationMs)
	assert.Equal(t, "com.example.Service.render", tree.Methods[r.MethodIdx])
	assert.Equal(t, int64(7), r.EnterMsRel)
	assert.Equal(t, int64(1), r.DurationMs)
	assert.Empty(t, tree.Params)
}

func TestBuildDeltaContinuation(t *testing.T) {
	// A delta over 31 ms does not fit the header's five bits and spills into
	// the varint continuation; the reconstructed time must not change.
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(1000, 1), wire.Exit(500),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)
	assert.Equal(t, int64(500), tree.Root.DurationMs)
}

func TestBuildTailAndHeadNoise(t *testing.T) {
	// §4.5: the first chunk starts with the previous call's tail (its
	// depth-0 EXIT included), and the last chunk carries the next call's
	// head. record_index = 2 points at the wanted ENTER; the tail's deltas
	// still advance the clock, the head is never visited.
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Tag(1, 4, "prev"), wire.Exit(2), // tail of the previous call
		wire.Enter(3, 1), wire.Exit(10), // the call itself: enter at +6, exit at +16
		wire.Enter(1, 2), wire.Tag(0, 4, "next"), // head of the next call, no exit
	}})

	tree, err := Build(blob, 2, dictOpt())
	require.NoError(t, err)
	assert.Equal(t, []string{"com.example.Service.handle"}, tree.Methods)
	assert.Equal(t, int64(10), tree.Root.DurationMs)
	assert.Empty(t, tree.Root.Children, "head noise is not part of the tree")
	assert.Empty(t, tree.Params, "the tail's tag is skipped, the head's tag never parsed")
}

func TestBuildMultiChunk(t *testing.T) {
	// The call spans two chunks; each chunk re-accumulates its deltas from
	// the timer epoch, so the second chunk's exit at +50 gives the duration.
	blob := blobOf(t,
		wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs + 5, Events: []wire.TraceEvent{
			wire.Enter(5, 1),
			wire.Enter(1, 2),
		}},
		wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs + 30, Events: []wire.TraceEvent{
			wire.Exit(30),
			wire.Exit(20),
		}},
	)
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)
	assert.Equal(t, int64(45), tree.Root.DurationMs, "50 - 5")
	require.Len(t, tree.Root.Children, 1)
	assert.Equal(t, int64(1), tree.Root.Children[0].EnterMsRel)
	assert.Equal(t, int64(24), tree.Root.Children[0].DurationMs, "30 - 6")
}

func TestBuildParams(t *testing.T) {
	opts := dictOpt()
	opts.BigValue = func(stream string, seq int, offset int64) (string, bool) {
		if stream == "sql" && seq == 2 && offset == 17 {
			return "SELECT 1", true
		}
		return "", false
	}
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Tag(0, 4, "req-1"),
		wire.Tag(0, 4, "req-2"),
		wire.BigTag(0, 5, true, 2, 17),
		wire.BigTag(0, 6, false, 3, 40),
		wire.Exit(1),
	}})

	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)
	require.Len(t, tree.Root.Params, 3)

	reqId := tree.Root.Params[0]
	assert.Equal(t, "request.id", tree.Params[reqId.ParamIdx])
	assert.Equal(t, []string{"req-1", "req-2"}, reqId.Values, "same param id merges, order kept")
	assert.Empty(t, reqId.Unresolved)

	sql := tree.Root.Params[1]
	assert.Equal(t, "sql", tree.Params[sql.ParamIdx])
	assert.Equal(t, []string{"SELECT 1"}, sql.Values, "PARAM_BIG_DEDUP resolves from the sql stream")

	xml := tree.Root.Params[2]
	assert.Equal(t, "xml", tree.Params[xml.ParamIdx])
	assert.Equal(t, []string{"xml:3:40"}, xml.Values,
		"an unresolvable reference is marked, not silently dropped")
	assert.Equal(t, []int{0}, xml.Unresolved)
}

func TestBuildDictMiss(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 99), wire.Exit(1),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)
	assert.Equal(t, []string{"#99"}, tree.Methods, "a missing word keeps the placeholder of the list path")
}

func TestBuildErrors(t *testing.T) {
	t.Run("record_index not on an ENTER", func(t *testing.T) {
		blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
			wire.Tag(0, 4, "x"), wire.Enter(0, 1), wire.Exit(1),
		}})
		_, err := Build(blob, 0, dictOpt())
		assert.ErrorContains(t, err, "does not land on an ENTER")
	})
	t.Run("blob ends before the depth-0 exit", func(t *testing.T) {
		blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
			wire.Enter(0, 1),
		}})
		_, err := Build(blob, 0, dictOpt())
		assert.ErrorContains(t, err, "before the depth-0 exit")
	})
}

func TestCollectBigRefs(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.BigTag(0, 5, true, 9, 9), wire.Exit(0), // tail noise of the previous call
		wire.Enter(0, 1),
		wire.BigTag(0, 5, true, 2, 17),
		wire.BigTag(0, 6, false, 3, 40),
		wire.Exit(1),
		wire.BigTag(0, 5, true, 8, 8), // head noise of the next call
	}})
	refs, err := CollectBigRefs(blob, 2)
	require.NoError(t, err)
	assert.Equal(t, []BigRef{
		{Stream: "sql", Seq: 2, Offset: 17},
		{Stream: "xml", Seq: 3, Offset: 40},
	}, refs, "only the call's own references; noise on both sides excluded")
	assert.Equal(t, "sql:2:17", refs[0].String())
}
