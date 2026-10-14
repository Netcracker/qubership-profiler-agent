package model

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func window(filters ...string) url.Values {
	return url.Values{"from": {"1"}, "to": {"2"}, "filter": filters}
}

func TestParseCallsQueryAcceptsStageAFilters(t *testing.T) {
	q, detail := ParseCallsQuery(window(
		"namespace=prod", "service in billing,orders", "pod=billing-1", "duration between 100ms..2s",
		"duration>1s", "error=false", "retention_class=any_error", "trace_id=4bf9", "request_id=abc123",
		"user.tenant=acme", "method=~Order(Service|Repository)",
	))
	require.Empty(t, detail)
	assert.Len(t, q.Filters, 11)

	q, detail = ParseCallsQuery(url.Values{
		"from": {"1"}, "to": {"2"}, "pod": {"ns/svc/pod"}, "filter": {"request_id=abc123"},
	})
	require.Empty(t, detail)
	assert.Equal(t, []Condition{PodTuplesCondition("ns/svc/pod"), {"request_id", OpEq, []string{"abc123"}}},
		q.Filters, "legacy parameters and filter mix")
}

func TestParseCallsQueryRejectsWhatStageADoesNotImplement(t *testing.T) {
	for text, message := range map[string]string{
		"service!=billing":   "!= on service is not implemented",
		"pod not in a":       "not in on pod is not implemented",
		"namespace=~prod":    "=~ on namespace is not implemented",
		"method!~Order":      "!~ on method is not implemented",
		"method=x":           "= on method is not implemented",
		"class=Foo":          "the field class is not implemented",
		"endpoint=GET /x":    "the field endpoint is not implemented",
		"user.tenant in a,b": "in on user.tenant is not implemented",
		"trace_id=None":      "the value None is not implemented",
		"user.tenant!=Any":   "!= on user.tenant is not implemented",
		"duration=1s":        "does not take the operator =",
		"duration>soon":      "is not a duration",
		"service":            "has no operator",
	} {
		_, detail := ParseCallsQuery(window(text))
		assert.Contains(t, detail, message, text)
		assert.Contains(t, detail, `filter "`+text+`"`, "the detail quotes the condition")
	}
}

func TestValuesSendsTheConditionsWithoutALegacyForm(t *testing.T) {
	q, detail := ParseCallsQuery(url.Values{
		"from": {"1"}, "to": {"2"}, "method": {"Svc"},
		"filter": {"duration>=100ms", "error=true", "retention_class=any_error", "service=billing", "request_id=abc", "error=false"},
	})
	require.Empty(t, detail)
	v := q.Values()
	assert.Equal(t, "Svc", v.Get("method"))
	assert.Equal(t, "100", v.Get("duration_min_ms"))
	assert.Equal(t, "true", v.Get("error_only"))
	assert.Equal(t, []string{"any_error"}, v["retention_class"])
	assert.Equal(t, []string{"method=~Svc", "service=billing", "request_id=abc", "error=false"}, v["filter"],
		"the method condition also goes as filter, so a regular expression is never lost")

	back, detail := ParseCallsQuery(v)
	require.Empty(t, detail, "a collector of this build accepts what query sends")
	assert.Equal(t, CanonicalConditions(q.Filters), CanonicalConditions(back.Filters),
		"retention_class=any_error comes back as the legacy retention_class, the same condition")
}

func TestIdentityPrefixes(t *testing.T) {
	for _, tc := range []struct {
		texts []string
		want  []string
		ok    bool
	}{
		{nil, nil, false},
		{[]string{"service=billing"}, nil, false},
		{[]string{"namespace=prod"}, []string{"prod"}, true},
		{[]string{"namespace in prod,stage", "service=billing"}, []string{"prod/billing", "stage/billing"}, true},
		{[]string{"namespace=prod", "service=billing", "pod in b-1,b-2"}, []string{"prod/billing/b-1", "prod/billing/b-2"}, true},
		{[]string{"namespace=prod", "pod=b-1"}, []string{"prod"}, true},
		{[]string{"namespace=prod", "namespace=stage"}, []string{}, true},
	} {
		got, ok := conds(t, tc.texts...).IdentityPrefixes()
		assert.Equal(t, tc.ok, ok, "%v", tc.texts)
		if tc.ok {
			assert.Equal(t, tc.want, got, "%v", tc.texts)
		}
	}
	assert.True(t, conds(t, "service=billing").IdentityFilter())
	assert.False(t, conds(t, "service!=billing").IdentityFilter())
	assert.False(t, conds(t, "user.tenant=acme").IdentityFilter())
}

func TestCapabilitiesMatchTheGate(t *testing.T) {
	caps := Capabilities()
	assert.Equal(t, []Op{OpEq, OpIn}, caps.Fields["service"])
	assert.Equal(t, []Op{OpEq}, caps.ParamOperators)
	assert.Equal(t, []Op{OpMatch}, caps.Fields["method"])
	assert.NotContains(t, caps.Fields, "class")
	assert.NotContains(t, caps.Fields, FieldPodTuple)
	for field, ops := range caps.Fields {
		for _, op := range ops {
			assert.NoError(t, checkImplemented(Condition{field, op, []string{"x"}}), "%s %s", field, op)
		}
	}
	caps.Fields["service"][0] = OpNe
	assert.Equal(t, OpEq, Capabilities().Fields["service"][0], "a caller cannot change the gate")
}
