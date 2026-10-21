package model

import (
	"math"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func conds(t *testing.T, texts ...string) CallsQuery {
	t.Helper()
	q := CallsQuery{FromMs: 1, ToMs: 2}
	for _, text := range texts {
		c, err := ParseCondition(text)
		require.NoError(t, err, text)
		q.Filters = append(q.Filters, c)
	}
	return q
}

func TestDurationBoundsIntersectEveryDurationCondition(t *testing.T) {
	for _, tc := range []struct {
		texts    []string
		min, max int64
	}{
		{nil, 0, math.MaxInt64},
		{[]string{"duration>100ms"}, 101, math.MaxInt64},
		{[]string{"duration>=100ms"}, 100, math.MaxInt64},
		{[]string{"duration<2s"}, 0, 1999},
		{[]string{"duration<=2s"}, 0, 2000},
		{[]string{"duration between 2s..100ms"}, 100, 2000},
		{[]string{"duration>=1s", "duration between 100ms..5s", "duration<3s"}, 1000, 2999},
		{[]string{"service=x"}, 0, math.MaxInt64},
	} {
		minMs, maxMs := conds(t, tc.texts...).DurationBounds()
		assert.Equal(t, [2]int64{tc.min, tc.max}, [2]int64{minMs, maxMs}, "%v", tc.texts)
	}
}

func TestRetentionClassSetIntersectsInTheCanonicalOrder(t *testing.T) {
	_, ok := conds(t).RetentionClassSet()
	assert.False(t, ok, "no condition, no constraint")

	set, ok := conds(t, "retention_class in any_error,short_clean", "retention_class in short_clean,any_error,corrupted").RetentionClassSet()
	assert.True(t, ok)
	assert.Equal(t, []string{RetentionShortClean, RetentionAnyError}, set)

	set, ok = conds(t, "retention_class=any_error", "retention_class=corrupted").RetentionClassSet()
	assert.True(t, ok)
	assert.Empty(t, set, "contradicting conditions allow no class")

	_, ok = conds(t, "retention_class!=any_error").RetentionClassSet()
	assert.False(t, ok, "a negation selects no prefix")
}

func TestErrorRequired(t *testing.T) {
	assert.True(t, conds(t, "error=true").ErrorRequired())
	assert.False(t, conds(t, "error=false").ErrorRequired())
	assert.False(t, conds(t).ErrorRequired())
}

func TestParseCallsQueryTurnsLegacyParametersIntoConditions(t *testing.T) {
	q, detail := ParseCallsQuery(url.Values{
		"from": {"1"}, "to": {"2"},
		"pod":             {"ns/a/p1", "ns/b/p2"},
		"method":          {"Svc.handle"},
		"retention_class": {"any_error", "short_clean"},
		"duration_min_ms": {"100"},
		"duration_max_ms": {"0"},
		"error_only":      {"true"},
	})
	require.Empty(t, detail)
	assert.Equal(t, []Condition{
		PodTuplesCondition("ns/a/p1", "ns/b/p2"),
		MethodContainsCondition("Svc.handle"),
		RetentionClassCondition("any_error", "short_clean"),
		DurationCondition(OpGe, 100),
		ErrorCondition(true),
	}, q.Filters, "duration_max_ms=0 means no bound")

	_, detail = ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "pod": {"ns/p"}})
	assert.Contains(t, detail, "pod must be")
	_, detail = ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "retention_class": {"hot"}})
	assert.Contains(t, detail, "unknown retention_class")
}

// TestValuesRoundTripsTheLegacyParameters checks that the fan-out to a
// collector carries a legacy query unchanged, so a collector of any version
// filters it the same way.
func TestValuesRoundTripsTheLegacyParameters(t *testing.T) {
	params := url.Values{
		"from": {"1"}, "to": {"2"},
		"pod":             {"ns/b/p2", "ns/a/p1"},
		"method":          {"Svc.handle(long)"},
		"retention_class": {"short_clean", "any_error"},
		"duration_min_ms": {"100"},
		"duration_max_ms": {"900"},
		"error_only":      {"true"},
	}
	q, detail := ParseCallsQuery(params)
	require.Empty(t, detail)
	back, detail := ParseCallsQuery(q.Values())
	require.Empty(t, detail)
	assert.Equal(t, CanonicalConditions(q.Filters), CanonicalConditions(back.Filters))
	assert.Equal(t, []string{"ns/b/p2", "ns/a/p1"}, q.Values()["pod"], "pods keep their order")
}

func TestRegexLiteral(t *testing.T) {
	for pattern, want := range map[string]string{
		"Svc":                 "Svc",
		`Svc\.handle\(long\)`: "Svc.handle(long)",
		`a\\b`:                `a\b`,
		"":                    "",
	} {
		got, ok := regexLiteral(pattern)
		assert.True(t, ok, pattern)
		assert.Equal(t, want, got, pattern)
	}
	for _, pattern := range []string{"Svc.*", `a\d`, `trailing\`, "^Svc"} {
		_, ok := regexLiteral(pattern)
		assert.False(t, ok, pattern)
	}
}

// TestLegacyMethodAnyAndNoneStaySubstringSearches covers method=Any and
// method=None, which the API and a collector both accept as a substring
// search, and the fan-out carries to a collector unchanged.
func TestLegacyMethodAnyAndNoneStaySubstringSearches(t *testing.T) {
	for _, method := range []string{"Any", "None"} {
		q, detail := ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "method": {method}})
		require.Empty(t, detail, method)
		assert.Equal(t, []Condition{{Field: "method", Op: OpMatch, Values: []string{method}}}, q.Filters, method)

		v := q.Values()
		assert.Equal(t, method, v.Get("method"), "the collector gets the same substring")
		back, detail := ParseCallsQuery(v)
		require.Empty(t, detail, method)
		assert.Equal(t, q.Filters, back.Filters, method)
	}
}

func TestLegacyDurationMaxZeroAddsNoCondition(t *testing.T) {
	q, detail := ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "duration_max_ms": {"0"}})
	require.Empty(t, detail)
	assert.Empty(t, q.Filters, "duration_max_ms=0 means no upper bound")
	_, maxMs := q.DurationBounds()
	assert.Equal(t, int64(math.MaxInt64), maxMs)
	assert.Empty(t, q.Values().Get("duration_max_ms"))
}

// TestDurationBoundsAboveTheInt32Maximum checks the bounds the stores push
// down for thresholds past the largest call duration: each one selects the
// same calls as the condition.
func TestDurationBoundsAboveTheInt32Maximum(t *testing.T) {
	const aboveMax = int64(math.MaxInt32) + 1
	for _, tc := range []struct {
		text     string
		min, max int64
	}{
		{"duration<2147483648ms", 0, math.MaxInt32},
		{"duration<=2147483648ms", 0, aboveMax},
		{"duration>=2147483648ms", aboveMax, math.MaxInt64},
		{"duration>2147483647ms", aboveMax, math.MaxInt64},
		{"duration>99999999999999999999999s", aboveMax + 1, math.MaxInt64},
		{"duration between 2147483648ms..2147483649ms", aboveMax, aboveMax},
	} {
		q := conds(t, tc.text)
		minMs, maxMs := q.DurationBounds()
		assert.Equal(t, [2]int64{tc.min, tc.max}, [2]int64{minMs, maxMs}, tc.text)

		// The collector reads the bounds as int32 parameters, so a bound past
		// the int32 maximum is left out and the full matcher applies it.
		_, detail := ParseCallsQuery(q.Values())
		assert.Empty(t, detail, tc.text)
	}
}
