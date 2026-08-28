package pipe

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The suspend and params streams are phrase-framed: every phrase is a fixed-int
// byte length followed by that many body bytes. The first phrase's body opens
// with a header — the 8-byte base time (suspend) or the 1-byte format version
// (params) — that later phrases omit. The header bytes are part of the first
// phrase's byte count, so the reader must charge them against lengthOfPhrase;
// otherwise it over-runs the first phrase and decodes the next phrase's length
// prefix as varint record data.
//
// The real agent emits one suspend phrase per 5-second flush window that saw a
// stop-the-world pause (PhraseOutputStream flushes accumulated phrases), so any
// long-running agent produces a multi-phrase suspend stream. These tests pin
// the framing across the phrase boundary.

// TestSuspendPipeReaderMultiPhrase feeds a two-phrase suspend stream: the first
// phrase carries the 8-byte base time plus two pauses, the second carries two
// more pauses with no header. All four must decode with the running timestamp
// continuing across the boundary.
func TestSuspendPipeReaderMultiPhrase(t *testing.T) {
	ctx := context.Background()
	const baseMs = 1_600_000_000_000

	data := encodeSuspendPhrases(baseMs, [][]suspendEvt{
		{{delta: 10, amount: 5}, {delta: 20, amount: 7}},
		{{delta: 30, amount: 9}, {delta: 40, amount: 11}},
	})

	var got []SuspendItem
	for item := range SuspendPipeReader(ctx, NewPipeReader(bytes.NewReader(data), false)) {
		got = append(got, item)
	}

	require.Len(t, got, 4, "both phrases decode; the reader must not lose framing after phrase 1")
	// cTime accumulates deltas from the base across the phrase boundary.
	assert.Equal(t, int64(baseMs+10), got[0].Time.UnixMilli())
	assert.Equal(t, 5, int(got[0].Amount))
	assert.Equal(t, int64(baseMs+30), got[1].Time.UnixMilli())
	assert.Equal(t, 7, int(got[1].Amount))
	assert.Equal(t, int64(baseMs+60), got[2].Time.UnixMilli())
	assert.Equal(t, 9, int(got[2].Amount))
	assert.Equal(t, int64(baseMs+100), got[3].Time.UnixMilli())
	assert.Equal(t, 11, int(got[3].Amount))
}

// TestSuspendPipeReaderHeaderOnlyFirstPhrase pins the empty-window case: the
// agent opens the suspend stream by emitting the base time, and a flush window
// with no pause flushes that header as a phrase of its own. The reader must
// step over the header-only phrase and frame the next phrase's records.
func TestSuspendPipeReaderHeaderOnlyFirstPhrase(t *testing.T) {
	ctx := context.Background()
	const baseMs = 1_600_000_000_000

	data := encodeSuspendPhrases(baseMs, [][]suspendEvt{
		{}, // header only: base time, no pauses this window
		{{delta: 15, amount: 3}},
	})

	var got []SuspendItem
	for item := range SuspendPipeReader(ctx, NewPipeReader(bytes.NewReader(data), false)) {
		got = append(got, item)
	}

	require.Len(t, got, 1)
	assert.Equal(t, int64(baseMs+15), got[0].Time.UnixMilli())
	assert.Equal(t, 3, int(got[0].Amount))
}

// TestParamsPipeReaderMultiPhrase feeds a two-phrase params stream: the first
// phrase carries the version byte plus one record, the second carries one more
// record with no header. Both records must decode.
func TestParamsPipeReaderMultiPhrase(t *testing.T) {
	ctx := context.Background()

	data := encodeParamsPhrases([][]paramRec{
		{{name: "userId", isIndex: true, order: 1, signature: "java.lang.String"}},
		{{name: "orderId", isList: true, order: 2, signature: "java.lang.Long"}},
	})

	var got []ParamItem
	for item := range ParamsPipeReader(ctx, NewPipeReader(bytes.NewReader(data), false)) {
		got = append(got, item)
	}

	require.Len(t, got, 2, "both phrases decode; the version byte must be charged against phrase 1")
	assert.Equal(t, "userId", got[0].Name)
	assert.True(t, got[0].IsIndex)
	assert.Equal(t, 1, got[0].Order)
	assert.Equal(t, "java.lang.String", got[0].Signature)
	assert.Equal(t, "orderId", got[1].Name)
	assert.True(t, got[1].IsList)
	assert.Equal(t, 2, got[1].Order)
	assert.Equal(t, "java.lang.Long", got[1].Signature)
}

// TestDictionaryPipeReaderMultiPhrase pins that the dictionary reader — which
// has no in-phrase header — keeps framing a two-phrase stream. Guards against a
// regression if the header accounting is ever copied here by mistake.
func TestDictionaryPipeReaderMultiPhrase(t *testing.T) {
	ctx := context.Background()

	data := append(encodeDictionaryPhrase([]string{"method.a", "param.a"}),
		encodeDictionaryPhrase([]string{"method.b"})...)

	byID := map[int]string{}
	for item := range DictionaryPipeReader(ctx, NewPipeReader(bytes.NewReader(data), false), 1000) {
		byID[item.Id] = item.Value
	}

	require.Len(t, byID, 3)
	assert.Equal(t, "method.a", byID[0])
	assert.Equal(t, "param.a", byID[1])
	assert.Equal(t, "method.b", byID[2], "the second phrase frames after the first")
}

// --- local encoders (mirror the agent's PhraseOutputStream) ------------------

type suspendEvt struct {
	delta  int
	amount int
}

type paramRec struct {
	name      string
	isIndex   bool
	isList    bool
	order     int
	signature string
}

// encodeSuspendPhrases frames each phrase as [fixed-int length][body]. The
// first body opens with the 8-byte base time; every body then holds
// (delta, amount) varint pairs (backend/libs/parser/pipe/suspend.go).
func encodeSuspendPhrases(baseMs uint64, phrases [][]suspendEvt) []byte {
	var out bytes.Buffer
	for i, evts := range phrases {
		var body bytes.Buffer
		if i == 0 {
			var eight [8]byte
			binary.BigEndian.PutUint64(eight[:], baseMs)
			body.Write(eight[:])
		}
		for _, e := range evts {
			putUvarint(&body, uint64(e.delta))
			putUvarint(&body, uint64(e.amount))
		}
		writePhraseFrame(&out, body.Bytes())
	}
	return out.Bytes()
}

// encodeParamsPhrases frames each phrase as [fixed-int length][body]. The first
// body opens with the format version byte; every record is
// var-string name, bool isIndex, bool isList, varint order, var-string
// signature (backend/libs/parser/pipe/params.go).
func encodeParamsPhrases(phrases [][]paramRec) []byte {
	var out bytes.Buffer
	for i, recs := range phrases {
		var body bytes.Buffer
		if i == 0 {
			body.WriteByte(1) // format version
		}
		for _, r := range recs {
			body.Write(encodeVarString(r.name))
			body.WriteByte(boolByte(r.isIndex))
			body.WriteByte(boolByte(r.isList))
			putUvarint(&body, uint64(r.order))
			body.Write(encodeVarString(r.signature))
		}
		writePhraseFrame(&out, body.Bytes())
	}
	return out.Bytes()
}

func writePhraseFrame(out *bytes.Buffer, body []byte) {
	var lenb [4]byte
	binary.BigEndian.PutUint32(lenb[:], uint32(len(body)))
	out.Write(lenb[:])
	out.Write(body)
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
