package calltree

import (
	"bytes"
	"encoding/binary"
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
	7: "binds",
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
	assert.Equal(t, int64(15), root.DurationMs, "exit at +20 minus enter at +5")
	assert.Equal(t, int64(10), root.SelfDurationMs, "15 total minus 4+1 in children")
	assert.Equal(t, int64(3), root.Executions, "itself plus one invocation of each child")
	assert.Equal(t, int64(1), root.SelfExecutions)
	require.Len(t, root.Children, 2)

	q, r := root.Children[0], root.Children[1]
	assert.Equal(t, "com.example.Service.query", tree.Methods[q.MethodIdx])
	assert.Equal(t, int64(4), q.DurationMs)
	assert.Equal(t, int64(4), q.SelfDurationMs, "a leaf's self equals its total")
	assert.Equal(t, "com.example.Service.render", tree.Methods[r.MethodIdx])
	assert.Equal(t, int64(1), r.DurationMs)
	assert.Empty(t, tree.Params)
}

func TestBuildRejectsOversizedTagStringLength(t *testing.T) {
	// A corrupt inline-tag var-string length must fail the decode, never panic.
	// A uint64 past MaxInt64 would wrap negative and make([]rune, n) would
	// panic; a merely huge length would over-allocate. Both must be errors.
	buf := &bytes.Buffer{}
	putLong := func(v uint64) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		buf.Write(b[:])
	}
	putLong(uint64(timerStartMs))       // timer epoch
	putLong(7)                          // threadId
	putLong(uint64(timerStartMs))       // chunk startMs
	buf.Write([]byte{0x00, 0x01})       // ENTER, delta 0, method id 1
	buf.Write([]byte{0x02, 0x04, 0x00}) // TAG, delta 0, tag id 4, PARAM_INLINE
	prefix := buf.Bytes()

	for _, tc := range []struct {
		name  string
		runes []byte
	}{
		// 2^63: fits uint64 but overflows int64 — the panic case.
		{"overflows int64", []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
		// 2^40: a valid int64 far past the bytes actually present.
		{"exceeds remaining bytes", []byte{0x80, 0x80, 0x80, 0x80, 0x20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := append(append([]byte(nil), prefix...), tc.runes...)
			blob = append(blob, 0x03) // EVENT_FINISH_RECORD
			require.NotPanics(t, func() {
				_, err := Build(blob, 0, dictOpt())
				require.Error(t, err)
			})
		})
	}
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
	assert.Equal(t, int64(21), tree.Root.SelfDurationMs, "45 minus the child's 24")
	require.Len(t, tree.Root.Children, 1)
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
	require.Len(t, reqId.Groups, 2, "distinct exact values are distinct groups")
	assert.Equal(t, ParamGroup{Value: "req-1", DurationMs: 1, Executions: 1}, reqId.Groups[0])
	assert.Equal(t, ParamGroup{Value: "req-2", DurationMs: 1, Executions: 1}, reqId.Groups[1])

	sql := tree.Root.Params[1]
	assert.Equal(t, "sql", tree.Params[sql.ParamIdx])
	require.Len(t, sql.Groups, 1)
	assert.Equal(t, "SELECT 1", sql.Groups[0].Value, "PARAM_BIG_DEDUP resolves from the sql stream")
	assert.False(t, sql.Groups[0].Unresolved)

	xml := tree.Root.Params[2]
	assert.Equal(t, "xml", tree.Params[xml.ParamIdx])
	require.Len(t, xml.Groups, 1)
	assert.Equal(t, "xml:3:40", xml.Groups[0].Value,
		"an unresolvable reference is marked, not silently dropped")
	assert.True(t, xml.Groups[0].Unresolved)
}

// TestBuildMergesSiblingInvocations pins the R5 merge semantics
// (08-ui-backend-requirements.md): three invocations of query under handle —
// two of them calling render — fold into one query node with one render
// child, every metric summed across the folded invocations.
func TestBuildMergesSiblingInvocations(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),                       // handle at +0
		wire.Enter(1, 2), wire.Tag(0, 4, "q1"), // query #1 at +1
		wire.Enter(1, 3), wire.Exit(2), // its render, +2..+4
		wire.Exit(1),                   // query #1 exits at +5: total 4, self 2
		wire.Enter(2, 2),               // query #2 at +7
		wire.Enter(1, 3), wire.Exit(1), // its render, +8..+9
		wire.Exit(3),                           // query #2 exits at +12: total 5, self 4
		wire.Enter(1, 2), wire.Tag(0, 4, "q3"), // query #3 at +13, no render
		wire.Exit(4), // query #3 exits at +17: total 4, self 4
		wire.Exit(3), // handle exits at +20
	}})

	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)

	root := tree.Root
	assert.Equal(t, int64(20), root.DurationMs)
	assert.Equal(t, int64(7), root.SelfDurationMs, "20 total minus 4+5+4 in query invocations")
	assert.Equal(t, int64(1), root.SelfExecutions)
	assert.Equal(t, int64(6), root.Executions, "1 handle + 3 query + 2 render")
	require.Len(t, root.Children, 1, "three sibling query invocations fold into one node")

	q := root.Children[0]
	assert.Equal(t, "com.example.Service.query", tree.Methods[q.MethodIdx])
	assert.Equal(t, int64(3), q.SelfExecutions)
	assert.Equal(t, int64(5), q.Executions)
	assert.Equal(t, int64(13), q.DurationMs, "4+5+4 across the folded invocations")
	assert.Equal(t, int64(10), q.SelfDurationMs, "13 total minus 2+1 in render")
	require.Len(t, q.Params, 1)
	require.Len(t, q.Params[0].Groups, 2, "folded invocations aggregate their values per group")
	assert.Equal(t, ParamGroup{Value: "q1", DurationMs: 4, Executions: 1}, q.Params[0].Groups[0],
		"the group carries its own invocation's duration")
	assert.Equal(t, ParamGroup{Value: "q3", DurationMs: 4, Executions: 1}, q.Params[0].Groups[1])

	require.Len(t, q.Children, 1)
	r := q.Children[0]
	assert.Equal(t, "com.example.Service.render", tree.Methods[r.MethodIdx])
	assert.Equal(t, int64(2), r.SelfExecutions)
	assert.Equal(t, int64(2), r.Executions)
	assert.Equal(t, int64(3), r.DurationMs)
	assert.Equal(t, int64(3), r.SelfDurationMs)

	assertMergeInvariants(t, root)
}

// TestBuildMergeKeepsDistinctSiblings pins what the merge must NOT do: only
// same-method siblings fold; an a-b-a interleave keeps two nodes in
// first-seen order, with the a invocations folded.
func TestBuildMergeKeepsDistinctSiblings(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(1, 2), wire.Exit(1), // a
		wire.Enter(1, 3), wire.Exit(1), // b
		wire.Enter(1, 2), wire.Exit(1), // a again
		wire.Exit(1),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)

	require.Len(t, tree.Root.Children, 2)
	a, b := tree.Root.Children[0], tree.Root.Children[1]
	assert.Equal(t, "com.example.Service.query", tree.Methods[a.MethodIdx])
	assert.Equal(t, int64(2), a.SelfExecutions)
	assert.Equal(t, "com.example.Service.render", tree.Methods[b.MethodIdx])
	assert.Equal(t, int64(1), b.SelfExecutions)
	assertMergeInvariants(t, tree.Root)
}

// TestBuildMergeRecursion pins that recursion keeps its depth structure: a
// self-recursive chain merges per level, never into its own ancestor.
func TestBuildMergeRecursion(t *testing.T) {
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(1, 1), wire.Exit(1), // recurse #1
		wire.Enter(1, 1),               // recurse #2 ...
		wire.Enter(1, 1), wire.Exit(1), // ... goes one deeper
		wire.Exit(1),
		wire.Exit(1),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)

	root := tree.Root
	assert.Equal(t, int64(1), root.SelfExecutions)
	require.Len(t, root.Children, 1, "both recursive invocations fold at depth 1")
	assert.Equal(t, int64(2), root.Children[0].SelfExecutions)
	require.Len(t, root.Children[0].Children, 1)
	assert.Equal(t, int64(1), root.Children[0].Children[0].SelfExecutions)
	assert.Equal(t, int64(4), root.Executions)
	assertMergeInvariants(t, root)
}

// TestBuildMergeHotspotRanking drives the merged tree through the flat
// profile the UI's Hotspots tab computes — self time aggregated by method —
// and pins the ranking a known synthetic trace must produce (07-ui-design.md
// §5.3).
func TestBuildMergeHotspotRanking(t *testing.T) {
	// handle self 7, query self 10, render self 3 (the merge-test fixture).
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(1, 2), wire.Enter(1, 3), wire.Exit(2), wire.Exit(1),
		wire.Enter(2, 2), wire.Enter(1, 3), wire.Exit(1), wire.Exit(3),
		wire.Enter(1, 2), wire.Exit(4),
		wire.Exit(3),
	}})
	tree, err := Build(blob, 0, dictOpt())
	require.NoError(t, err)

	selfByMethod := map[string]int64{}
	var walk func(n *Node)
	walk = func(n *Node) {
		selfByMethod[tree.Methods[n.MethodIdx]] += n.SelfDurationMs
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(tree.Root)

	assert.Equal(t, map[string]int64{
		"com.example.Service.query":  10,
		"com.example.Service.handle": 7,
		"com.example.Service.render": 3,
	}, selfByMethod, "the hotspot profile ranks query > handle > render")
}

// TestBuildSuspensionAttribution pins the R7 semantics
// (08-ui-backend-requirements.md): each node's suspension is its work
// interval intersected with the global timeline, and a pause spanning a
// child's exit splits between the child and the parent's self time. The
// timeline is deliberately out of order — Build normalizes it.
func TestBuildSuspensionAttribution(t *testing.T) {
	// root [+0, +40) with one child [+10, +20).
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(10, 2), wire.Exit(10),
		wire.Exit(20),
	}})
	opts := dictOpt()
	// SuspendInterval.TimeMs is the pause END, so a pause spans
	// [TimeMs − DurationMs, TimeMs] (№4). The comments give that span.
	opts.Suspend = []SuspendInterval{
		{TimeMs: timerStartMs + 55, DurationMs: 5}, // [50, 55): past the root exit, ignored
		{TimeMs: timerStartMs + 22, DurationMs: 4}, // [18, 22): 2 ms child, 2 ms root self
		{TimeMs: timerStartMs + 7, DurationMs: 2},  // [5, 7): before the child, root self
		{TimeMs: timerStartMs + 15, DurationMs: 3}, // [12, 15): inside the child
	}

	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)

	root := tree.Root
	assert.Equal(t, int64(9), root.SuspensionMs, "2 + 3 + 4 of the pauses land inside [0, 40)")
	assert.Equal(t, int64(4), root.SelfSuspensionMs, "9 total minus the child's 5")
	require.Len(t, root.Children, 1)
	child := root.Children[0]
	assert.Equal(t, int64(5), child.SuspensionMs, "[12, 15) whole, [18, 22) clipped to the exit at +20")
	assert.Equal(t, int64(5), child.SelfSuspensionMs)
	assertMergeInvariants(t, root)
}

// TestBuildSuspensionAcrossMergedInvocations pins that suspension, like
// duration, sums per invocation before the R5 fold: one pause spanning the
// gap between two folded invocations counts only their work intervals.
func TestBuildSuspensionAcrossMergedInvocations(t *testing.T) {
	// root [+0, +20); q #1 [+2, +6), q #2 [+10, +14); pause [4, 12).
	blob := blobOf(t, wire.TraceChunk{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
		wire.Enter(0, 1),
		wire.Enter(2, 2), wire.Exit(4),
		wire.Enter(4, 2), wire.Exit(4),
		wire.Exit(6),
	}})
	opts := dictOpt()
	// The pause spans [4, 12): TimeMs is the end (№4).
	opts.Suspend = []SuspendInterval{{TimeMs: timerStartMs + 12, DurationMs: 8}}

	tree, err := Build(blob, 0, opts)
	require.NoError(t, err)

	root := tree.Root
	assert.Equal(t, int64(8), root.SuspensionMs)
	assert.Equal(t, int64(4), root.SelfSuspensionMs,
		"the [6, 10) middle of the pause falls between the invocations: root self time")
	require.Len(t, root.Children, 1)
	q := root.Children[0]
	assert.Equal(t, int64(4), q.SuspensionMs, "[4, 6) of #1 plus [10, 12) of #2")
	assert.Equal(t, int64(4), q.SelfSuspensionMs)
	assertMergeInvariants(t, root)
}

func TestNormalizeSuspend(t *testing.T) {
	// SuspendInterval.TimeMs is the pause END, so a pause spans
	// [TimeMs − DurationMs, TimeMs] (№4). Inputs span [30,35), [0,10), [5,25):
	// the last two overlap into one [0,25) pause.
	got := normalizeSuspend([]SuspendInterval{
		{TimeMs: 35, DurationMs: 5},
		{TimeMs: 10, DurationMs: 10},
		{TimeMs: 25, DurationMs: 20}, // overlaps the previous: one [0, 25) pause
	})
	assert.Equal(t, []SuspendInterval{
		{TimeMs: 25, DurationMs: 25}, // [0, 25)
		{TimeMs: 35, DurationMs: 5},  // [30, 35)
	}, got, "unsorted and overlapping pauses normalize; overlap is never double-counted")
}

// assertMergeInvariants checks the arithmetic every merged node must satisfy
// (02 §2.5.3): executions = selfExecutions + Σ children.executions,
// selfDurationMs = durationMs − Σ children.durationMs, and the same self
// arithmetic for suspension, which can also never exceed the duration.
func assertMergeInvariants(t *testing.T, n *Node) {
	t.Helper()
	childExecutions, childDuration, childSuspension := int64(0), int64(0), int64(0)
	for _, child := range n.Children {
		childExecutions += child.Executions
		childDuration += child.DurationMs
		childSuspension += child.SuspensionMs
		assertMergeInvariants(t, child)
	}
	assert.Equal(t, n.SelfExecutions+childExecutions, n.Executions)
	assert.Equal(t, n.DurationMs-childDuration, n.SelfDurationMs)
	assert.Equal(t, n.SuspensionMs-childSuspension, n.SelfSuspensionMs)
	assert.LessOrEqual(t, n.SuspensionMs, n.DurationMs)
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
