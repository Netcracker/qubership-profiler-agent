package model

import (
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
