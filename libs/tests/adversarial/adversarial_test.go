//go:build adversarial

// Package adversarial is the pure-Go counterpart of libs/tests/smoke_realagent.
// It feeds the actual Go decoder bytes shaped exactly as the Java agent's
// DataOutputStreamEx writes them — via the wire helpers, whose putVarString now
// emits faithful UTF-16 — and asserts the adversarial strings round-trip
// byte-exact. It needs no JDK and no docker: the wire helpers stand in for the
// agent and the pipe / calltree readers stand in for the backend.
//
// It is an acceptance gate for the decoder fixes and FAILS today on the same
// two bugs the real-agent E2E proves (scripts/e2e-realagent/README.md):
//
//   - Bug A — libs/parser/pipe/pipe_reader.go readChar (and the mirror at
//     libs/calltree/calltree.go) reads a signed int16, so every UTF-16 code
//     unit >= U+8000 (most CJK and Hangul) and both halves of a non-BMP
//     surrogate pair (emoji) decode to U+FFFD.
//   - Bug B — libs/parser/pipe/dictionary.go skips an empty dictionary word
//     without advancing its id counter, so every later id shifts down by one
//     and resolves to the wrong word.
//
// The failing assertions are the deliverable: do not change decoder code to
// make them pass. Run with
//
//	make -C backend adversarial
//
// or `go test -tags adversarial ./libs/tests/adversarial/...`.
package adversarial

import (
	"bytes"
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Adversarial glyphs, mirroring smoke_realagent and AdversarialMain.EXPECTED_*.
const (
	cjk    = "語"          // one UTF-16 code unit >= U+8000
	hangul = "한"          // one UTF-16 code unit >= U+8000
	emoji  = "\U0001F525" // 🔥 — non-BMP, a surrogate pair (both halves >= U+8000)
	glyphs = cjk + hangul + emoji
)

const timerStartMs = int64(1_700_000_000_000)

// TestBugAUnicodeRoundTrip pins the correct Unicode round-trip across the three
// string surfaces the real agent exercises: a dictionary word (method names and
// param keys) and an inline trace value (param values). Every one FAILS today —
// the >= U+8000 code units and the emoji surrogate pair come back as U+FFFD.
func TestBugAUnicodeRoundTrip(t *testing.T) {
	t.Run("dictionary method name", func(t *testing.T) {
		method := "void com.acme.Svc." + glyphs + "_handle() (AdversarialMain.java) [test-app.jar]"
		byId := decodeDictionary(t, []string{method})
		assert.Equal(t, method, byId[0],
			"BUG A: a dictionary word must round-trip byte-exact; readChar reads a "+
				"signed int16, so every code unit >= U+8000 and the emoji surrogate "+
				"pair decode to U+FFFD")
	})

	t.Run("dictionary param key", func(t *testing.T) {
		key := "param." + glyphs
		byId := decodeDictionary(t, []string{key})
		assert.Equal(t, key, byId[0], "BUG A: a param key must round-trip byte-exact")
	})

	t.Run("inline param value", func(t *testing.T) {
		const methodId, paramId = 1, 2
		value := "value-" + glyphs + "-tail"
		blob, _ := wire.TraceStream(timerStartMs, []wire.TraceChunk{
			{ThreadId: 7, StartMs: timerStartMs, Events: []wire.TraceEvent{
				wire.Enter(0, methodId),
				wire.Tag(0, paramId, value),
				wire.Exit(1),
			}},
		})
		tree, err := calltree.Build(blob, 0, calltree.Options{
			Dict: func(id int) (string, bool) {
				return map[int]string{methodId: "com.acme.Svc.handle", paramId: "param.key"}[id], true
			},
		})
		require.NoError(t, err)
		require.NotNil(t, tree.Root)
		require.Len(t, tree.Root.Params, 1)
		require.Len(t, tree.Root.Params[0].Groups, 1)
		assert.Equal(t, value, tree.Root.Params[0].Groups[0].Value,
			"BUG A: an inline param value must round-trip byte-exact; the calltree "+
				"walker mirrors the signed-int16 readChar")
	})
}

// TestBugBEmptyWordKeepsIdsAligned pins the id alignment the agent guarantees:
// it registers every dictionary word, including the empty string, so a later
// word keeps the id the agent gave it. It FAILS today — the reader drops the
// empty word without advancing its counter, so "param.b" arrives under the
// empty word's id (1) instead of its own (2).
func TestBugBEmptyWordKeepsIdsAligned(t *testing.T) {
	// The agent appends words by arrival order: "method.b" = 0, "" = 1,
	// "param.b" = 2.
	byId := decodeDictionary(t, []string{"method.b", "", "param.b"})

	assert.Equal(t, "method.b", byId[0], "the word before the empty one keeps id 0")
	assert.Equal(t, "param.b", byId[2],
		"BUG B: the empty word must consume id 1 so this word stays at id 2; the "+
			"reader skips the empty word without advancing the counter, so it "+
			"resolves to id 1 and every later id shifts by one")
}

// decodeDictionary encodes the words as the agent's dictionary phrase and
// decodes them back with the production reader, returning the resolved id → word
// map. An empty word yields no entry (that gap is exactly what Bug B is about).
func decodeDictionary(t *testing.T, words []string) map[int]string {
	t.Helper()
	data := wire.DictionaryStream(words)
	reader := pipe.NewPipeReader(bytes.NewReader(data), false)
	byId := map[int]string{}
	for item := range pipe.DictionaryPipeReader(context.Background(), reader, 1000) {
		byId[item.Id] = item.Value
	}
	return byId
}
