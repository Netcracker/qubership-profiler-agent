package parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDictWordsRoundTrip pins the dict_words_json shape both sides share: the
// seal writer encodes what the cold /tree reader decodes, id-keyed.
func TestDictWordsRoundTrip(t *testing.T) {
	col, err := EncodeDictWords(map[int]string{5: "com.example.Service.handle", 17: "request.id"})
	require.NoError(t, err)
	require.NotNil(t, col)

	words, err := DecodeDictWords(col)
	require.NoError(t, err)
	assert.Equal(t, map[int]string{5: "com.example.Service.handle", 17: "request.id"}, words)

	empty, err := EncodeDictWords(nil)
	require.NoError(t, err)
	assert.Nil(t, empty, "an empty subset encodes as a NULL column")

	decoded, err := DecodeDictWords(nil)
	require.NoError(t, err)
	assert.Empty(t, decoded, "a NULL column decodes to no words: every id renders as #<id>")

	bad := `{"not-an-id":"x"}`
	_, err = DecodeDictWords(&bad)
	assert.Error(t, err)
}

// TestSuspendRoundTrip pins the suspend_json shape: (end, duration) events,
// the №4 wire semantics.
func TestSuspendRoundTrip(t *testing.T) {
	events := []SuspendEvent{{EndMs: 1_000, DurationMs: 30}, {EndMs: 2_000, DurationMs: 20}}
	col, err := EncodeSuspend(events)
	require.NoError(t, err)
	require.NotNil(t, col)
	assert.JSONEq(t, `[{"end_ms":1000,"duration_ms":30},{"end_ms":2000,"duration_ms":20}]`, *col)

	decoded, err := DecodeSuspend(col)
	require.NoError(t, err)
	assert.Equal(t, events, decoded)

	empty, err := EncodeSuspend(nil)
	require.NoError(t, err)
	assert.Nil(t, empty, "an empty timeline encodes as a NULL column")

	decoded, err = DecodeSuspend(nil)
	require.NoError(t, err)
	assert.Empty(t, decoded, "a NULL column decodes to zero suspension")
}
