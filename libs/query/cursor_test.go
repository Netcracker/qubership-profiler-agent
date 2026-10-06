package query

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorRoundTrip(t *testing.T) {
	q := model.CallsQuery{FromMs: 100, ToMs: 200, Pods: []string{"ns/svc/pod"}, ErrorOnly: true}
	pos := model.Position{TsMs: 150, PK: model.PK{PodNamespace: "ns", PodName: "pod", RecordIndex: 3}}

	tok, err := decodeCursor(encodeCursor(q, pos), 15*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, q, tok.Query, "the frozen query survives the round trip")
	assert.Equal(t, pos, tok.Pos)
}

func TestCursorExpiry(t *testing.T) {
	cur := encodeCursor(model.CallsQuery{FromMs: 1, ToMs: 2}, model.Position{})

	_, err := decodeCursor(cur, 15*time.Minute)
	require.NoError(t, err, "a fresh cursor validates")

	clock.As(time.Now().Add(16*time.Minute), func() {
		_, err := decodeCursor(cur, 15*time.Minute)
		assert.ErrorContains(t, err, "expired", "past PROFILER_CURSOR_TTL the cursor is rejected (02 §2.3.1)")
	})
}

func TestCursorRejectsGarbage(t *testing.T) {
	_, err := decodeCursor("not base64 ***", time.Minute)
	assert.Error(t, err)

	_, err = decodeCursor("bm90IGpzb24", time.Minute) // "not json"
	assert.Error(t, err)

	// A version bump invalidates outstanding cursors.
	_, err = decodeCursor("eyJ2Ijo5OSwiaWF0IjoxfQ", time.Minute) // {"v":99,"iat":1}
	assert.ErrorContains(t, err, "version")
}
