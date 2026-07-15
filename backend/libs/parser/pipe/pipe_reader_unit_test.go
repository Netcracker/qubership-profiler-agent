package pipe

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"unicode/utf16"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadVarStringUnicodeRoundTrip pins №19: a var-string carries one UTF-16
// code unit per char, so a code unit >= U+8000 must survive and a non-BMP rune
// (a surrogate pair) must reassemble. The old signed-int16 readChar decoded
// both to U+FFFD.
func TestReadVarStringUnicodeRoundTrip(t *testing.T) {
	ctx := context.Background()
	for _, s := range []string{
		"ascii-only",
		"語",          // one code unit >= U+8000 (CJK)
		"한",          // one code unit >= U+8000 (Hangul)
		"\U0001F525", // 🔥 — non-BMP, a surrogate pair
		"value-語한🔥-tail",
		"", // the empty string round-trips too
	} {
		s := s
		t.Run(s, func(t *testing.T) {
			r := NewPipeReader(bytes.NewReader(encodeVarString(s)), false)
			gotLen, _, got := r.ReadVarString(ctx)
			assert.Equal(t, len(utf16.Encode([]rune(s))), gotLen, "length is the UTF-16 code-unit count")
			assert.Equal(t, s, got, "the string round-trips byte-exact")
		})
	}
}

// TestReadFixedStringLengthCap pins №13 on the parser path: a fixed-string
// length past DATA_BUFFER_SIZE is refused with an error, never allocated.
func TestReadFixedStringLengthCap(t *testing.T) {
	ctx := context.Background()

	t.Run("oversized length errors, not OOM", func(t *testing.T) {
		r := NewPipeReader(bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF}), false)
		got, err := r.ReadFixedString(ctx)
		require.Error(t, err)
		assert.Empty(t, got)
		assert.NotNil(t, r.Err(), "Err surfaces the cap breach so ingest can ACK_ERROR_MAGIC")
	})

	t.Run("length at the cap is accepted", func(t *testing.T) {
		var b bytes.Buffer
		_ = binary.Write(&b, binary.BigEndian, uint32(1024))
		b.Write(bytes.Repeat([]byte("x"), 1024))
		r := NewPipeReader(&b, false)
		got, err := r.ReadFixedString(ctx)
		require.NoError(t, err)
		assert.Len(t, got, 1024)
	})

	t.Run("length one past the cap is refused", func(t *testing.T) {
		var b bytes.Buffer
		_ = binary.Write(&b, binary.BigEndian, uint32(1025))
		r := NewPipeReader(&b, false)
		_, err := r.ReadFixedString(ctx)
		require.Error(t, err)
	})
}

// TestDictionaryPipeReaderEmptyWord pins №20: the reader registers EVERY word
// by arrival order, empty ones included, so a later word keeps the id the agent
// assigned it. Skipping the empty word used to shift every later id by one.
func TestDictionaryPipeReaderEmptyWord(t *testing.T) {
	ctx := context.Background()
	data := encodeDictionaryPhrase([]string{"method.b", "", "param.b"})
	r := NewPipeReader(bytes.NewReader(data), false)

	byId := map[int]string{}
	for item := range DictionaryPipeReader(ctx, r, 1000) {
		byId[item.Id] = item.Value
	}

	assert.Equal(t, "method.b", byId[0])
	assert.Equal(t, "", byId[1], "the empty word consumes id 1 instead of being dropped")
	assert.Equal(t, "param.b", byId[2], "later ids do not shift")
}

// --- local encoders (mirror the agent's DataOutputStreamEx) ------------------

// encodeVarString writes a var-string the way the agent does: a varint length
// that is the UTF-16 code-unit count, then two big-endian bytes per code unit.
func encodeVarString(s string) []byte {
	var b bytes.Buffer
	units := utf16.Encode([]rune(s))
	putUvarint(&b, uint64(len(units)))
	for _, u := range units {
		var two [2]byte
		binary.BigEndian.PutUint16(two[:], u)
		b.Write(two[:])
	}
	return b.Bytes()
}

// encodeDictionaryPhrase frames words as one PROTOCOL_VERSION_V2 phrase: a
// fixed-int byte length, then the var-strings (ids implied by arrival order).
func encodeDictionaryPhrase(words []string) []byte {
	var body bytes.Buffer
	for _, w := range words {
		body.Write(encodeVarString(w))
	}
	var out bytes.Buffer
	_ = binary.Write(&out, binary.BigEndian, uint32(body.Len()))
	out.Write(body.Bytes())
	return out.Bytes()
}

func putUvarint(b *bytes.Buffer, v uint64) {
	for v >= 0x80 {
		b.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	b.WriteByte(byte(v))
}
