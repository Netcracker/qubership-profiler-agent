package model

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func matchRow() CallRow {
	return CallRow{
		PK:   PK{PodNamespace: "prod", PodService: "billing", PodName: "billing-7f8c", RestartTimeMs: 1},
		TsMs: 150, DurationMs: 500,
		Method:         "void com.example.Billing.charge(long) (Billing.java) [billing.jar]",
		RetentionClass: RetentionNormalClean,
		Params: map[string][]string{
			"trace.id":     {"4bf92f3577b34da6a3ce929d0e0e4736"},
			"x-request-id": {"abc123"},
			"user.tenant":  {"acme", "globex"},
			"empty":        {},
		},
	}
}

// matches parses each condition, as a `filter` value would arrive, and runs
// the matcher over the row.
func matches(t *testing.T, r CallRow, texts ...string) bool {
	t.Helper()
	q := CallsQuery{FromMs: 100, ToMs: 200}
	for _, text := range texts {
		c, err := ParseCondition(text)
		require.NoError(t, err, text)
		q.Filters = append(q.Filters, c)
	}
	m, err := q.Matcher()
	require.NoError(t, err, texts)
	return m.Match(&r)
}

func TestMatcherOperators(t *testing.T) {
	r := matchRow()
	for text, want := range map[string]bool{
		"namespace=prod":                            true,
		"namespace=Prod":                            false, // case-sensitive
		"service!=billing":                          false,
		"service!=orders":                           true,
		"pod in billing-1,billing-7f8c":             true,
		"pod not in billing-7f8c":                   false,
		"pod=~7f":                                   true, // unanchored
		"pod=~^7f":                                  false,
		"pod!~7f":                                   false,
		"method=~Billing\\.charge":                  true,
		"retention_class=normal_clean":              true,
		"retention_class in any_error":              false,
		"duration>499ms":                            true,
		"duration>500ms":                            false,
		"duration>=500ms":                           true,
		"duration<500ms":                            false,
		"duration<=0.5s":                            true,
		"duration between 500ms..1s":                true,
		"duration between 1s..501ms":                false,
		"error=false":                               true,
		"error=true":                                false,
		"trace_id=4bf92f3577b34da6a3ce929d0e0e4736": true,
		"request_id=abc123":                         true,
		"request_id=~^abc":                          true,
		"x-request-id=abc123":                       true, // the param under its own name
		"user.tenant=globex":                        true, // any value of a multi-valued param
		"user.tenant!=globex":                       false,
		"user.tenant in initech,acme":               true,
		"user.tenant=~^glo":                         true,
		"user.tenant!~^glo":                         false,
		"user.tenant=Any":                           true,
		"user.tenant=None":                          false,
		"missing=x":                                 false,
		"missing!=x":                                true, // a negation matches a call without the field
		"missing not in x,y":                        true,
		"missing!~.":                                true,
		"missing=None":                              true,
		"missing!=None":                             false,
		"missing!=Any":                              true,
		"empty=None":                                true, // a param with no value counts as absent
	} {
		assert.Equal(t, want, matches(t, r, text), text)
	}
}

func TestMatcherNeedsEveryConditionAndTheWindow(t *testing.T) {
	r := matchRow()
	assert.True(t, matches(t, r, "service=billing", "request_id=abc123"))
	assert.False(t, matches(t, r, "service=billing", "request_id=other"))

	m, err := CallsQuery{FromMs: 151, ToMs: 200}.Matcher()
	require.NoError(t, err)
	assert.False(t, m.Match(&r), "ts below from")
	m, err = CallsQuery{FromMs: 100, ToMs: 150}.Matcher()
	require.NoError(t, err)
	assert.False(t, m.Match(&r), "to is exclusive")
}

func TestLegacyConditionsMatchLikeTheOldParameters(t *testing.T) {
	r := matchRow()
	match := func(c Condition) bool {
		m, err := CallsQuery{FromMs: 100, ToMs: 200, Filters: []Condition{c}}.Matcher()
		require.NoError(t, err)
		return m.Match(&r)
	}
	assert.True(t, match(MethodContainsCondition("Billing.charge(long)")), "a substring with regex syntax matches literally")
	assert.False(t, match(MethodContainsCondition("Billing.charge(int)")))
	assert.False(t, match(MethodContainsCondition("Billing.ch.rge")), "a dot is not a wildcard")
	assert.True(t, match(PodTuplesCondition("prod/other/x", "prod/billing/billing-7f8c")))
	assert.False(t, match(PodTuplesCondition("prod/billing/billing-1")))
	assert.True(t, match(DurationCondition(OpGe, 500)))
	assert.False(t, match(DurationCondition(OpLe, 499)))
	assert.False(t, match(ErrorCondition(true)))
	assert.True(t, match(RetentionClassCondition(RetentionShortClean, RetentionNormalClean)))
}

// TestNoneAndAnyAreLiteralOutsideEqualityOperators covers the legacy
// method=Any and method=None, which become method=~Any and method=~None: a
// substring search for the word, not a special value.
func TestNoneAndAnyAreLiteralOutsideEqualityOperators(t *testing.T) {
	r := matchRow()
	r.Method = "void com.example.AnyService.findNone()"
	r.Params["flag"] = []string{"None"}
	for text, want := range map[string]bool{
		"method=~Any":        true,
		"method=~None":       true,
		"method!~Any":        false,
		"method=~^Any":       false,
		"flag=~None":         true,
		"flag in Any,None":   true,
		"flag not in None":   false,
		"missing=~Any":       false,
		"trace_id=~None":     false,
		"user.tenant in Any": false,
	} {
		assert.Equal(t, want, matches(t, r, text), text)
	}
	for _, method := range []string{"Any", "None"} {
		other := matchRow()
		assert.True(t, match(t, r, MethodContainsCondition(method)), method)
		assert.False(t, match(t, other, MethodContainsCondition(method)), method)
	}
}

// match runs the matcher with one condition built in code, as the legacy
// parameters build it.
func match(t *testing.T, r CallRow, c Condition) bool {
	t.Helper()
	m, err := CallsQuery{FromMs: 100, ToMs: 200, Filters: []Condition{c}}.Matcher()
	require.NoError(t, err, c)
	return m.Match(&r)
}

// TestTraceIDReadsEveryTraceParam checks that trace_id finds a call by any of
// the names the UI shows a trace ID from (02 §2.3.3), and that the values of
// all of them count as values of one param.
func TestTraceIDReadsEveryTraceParam(t *testing.T) {
	row := func(params map[string][]string) CallRow {
		r := matchRow()
		r.Params = params
		return r
	}
	shared := row(map[string][]string{"trace.id": {"t1"}})
	brave := row(map[string][]string{"brave.trace_id": {"t2"}})
	b3 := row(map[string][]string{"X-B3-TraceId": {"t3"}})
	mixed := row(map[string][]string{"trace.id": {"t1"}, "X-B3-TraceId": {"t3"}})
	none := row(map[string][]string{"user.tenant": {"acme"}})

	for _, tc := range []struct {
		r    CallRow
		text string
		want bool
	}{
		{shared, "trace_id=t1", true},
		{brave, "trace_id=t2", true},
		{b3, "trace_id=t3", true},
		{b3, "trace_id=t1", false},
		{mixed, "trace_id=t1", true},
		{mixed, "trace_id=t3", true},
		{mixed, "trace_id!=t3", false}, // a negation holds only when no name has the value
		{mixed, "trace_id!=t2", true},
		{brave, "trace_id=~^t", true},
		{brave, "trace_id=Any", true},
		{b3, "trace_id=None", false},
		{none, "trace_id=None", true},
		{none, "trace_id!=t1", true},
		{brave, "brave.trace_id=t2", true}, // each name stays a plain param
		{brave, "trace.id=None", true},
	} {
		assert.Equal(t, tc.want, matches(t, tc.r, tc.text), "%s on %v", tc.text, tc.r.Params)
	}
}

// TestDurationThresholdsAboveTheInt32Maximum checks that a threshold past the
// largest duration a call can have keeps the meaning of its comparison.
func TestDurationThresholdsAboveTheInt32Maximum(t *testing.T) {
	r := matchRow()
	r.DurationMs = math.MaxInt32
	for text, want := range map[string]bool{
		"duration<2147483647ms":                       false,
		"duration<=2147483647ms":                      true,
		"duration>=2147483647ms":                      true,
		"duration>2147483647ms":                       false,
		"duration<2147483648ms":                       true,
		"duration<=2147483648ms":                      true,
		"duration>=2147483648ms":                      false,
		"duration>2147483648ms":                       false,
		"duration<99999999999999999999999s":           true,
		"duration>=99999999999999999999999s":          false,
		"duration between 2147483648ms..2147483649ms": false,
		"duration between 2147483647ms..2147483649ms": true,
		"duration between 1ms..2147483646ms":          false,
		"duration between 1ms..99999999999s":          true,
	} {
		assert.Equal(t, want, matches(t, r, text), text)
	}

	r.DurationMs = 0
	assert.True(t, matches(t, r, "duration<=0ms"), "an explicit duration<=0ms keeps its literal meaning")
	r.DurationMs = 1
	assert.False(t, matches(t, r, "duration<=0ms"))
}
