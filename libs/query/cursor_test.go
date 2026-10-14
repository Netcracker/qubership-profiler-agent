package query

import (
	"net/url"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorRoundTrip(t *testing.T) {
	params := url.Values{
		"from": {"100"}, "to": {"200"}, "pod": {"ns/svc/pod"}, "error_only": {"true"},
		"limit": {"10"}, "cursor": {"old"},
	}
	pos := model.Position{TsMs: 150, PK: model.PK{PodNamespace: "ns", PodName: "pod", RecordIndex: 3}}

	tok, err := decodeCursor(encodeCursor(params, pos), 15*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, url.Values{"from": {"100"}, "to": {"200"}, "pod": {"ns/svc/pod"}, "error_only": {"true"}}, tok.Params,
		"the cursor freezes the query parameters and nothing else")
	q, err := tok.query()
	require.NoError(t, err)
	assert.Equal(t, model.CallsQuery{FromMs: 100, ToMs: 200, Filters: []model.Condition{
		model.PodTuplesCondition("ns/svc/pod"), model.ErrorCondition(true),
	}}, q, "the frozen query survives the round trip")
	assert.Equal(t, pos, tok.Pos)
}

func TestCursorExpiry(t *testing.T) {
	cur := encodeCursor(url.Values{"from": {"1"}, "to": {"2"}}, model.Position{})

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

func TestFrozenQueryMismatch(t *testing.T) {
	frozen := url.Values{
		"from": {"100"}, "to": {"200"}, "pod": {"ns/a/p1", "ns/b/p2"}, "duration_min_ms": {"1000"},
	}
	frozenQ, detail := model.ParseCallsQuery(frozen)
	require.Empty(t, detail)

	for name, resent := range map[string]url.Values{
		"nothing re-sent":           {},
		"the same parameters":       frozen,
		"pods in another order":     {"pod": {"ns/b/p2", "ns/a/p1"}},
		"one parameter of several":  {"duration_min_ms": {"1000"}},
		"only the paging params":    {"limit": {"5"}, "cursor": {"x"}},
		"the window as page 1 sent": {"from": {"100"}, "to": {"200"}},
	} {
		assert.Empty(t, frozenQueryMismatch(frozen, frozenQ, resent), name)
	}
	for name, resent := range map[string]url.Values{
		"a different pod":       {"pod": {"ns/a/p1"}},
		"a new filter":          {"error_only": {"true"}},
		"a moved window":        {"to": {"300"}},
		"a malformed parameter": {"duration_min_ms": {"abc"}},
	} {
		assert.NotEmpty(t, frozenQueryMismatch(frozen, frozenQ, resent), name)
	}
}

func TestCursorWithAQueryThatDoesNotParseIsRejected(t *testing.T) {
	tok, err := decodeCursor(encodeCursor(url.Values{"from": {"200"}, "to": {"100"}}, model.Position{}), time.Minute)
	require.NoError(t, err)
	_, err = tok.query()
	assert.ErrorContains(t, err, "does not parse")
}
