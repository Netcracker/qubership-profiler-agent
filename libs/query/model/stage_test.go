package model

import (
	"fmt"
	"net/url"
	"runtime"
	"strings"
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

func TestParseCallsQueryAcceptsStageBFilters(t *testing.T) {
	q, detail := ParseCallsQuery(window(
		"service!=billing", "pod not in a,b", "namespace=~^prod", "method!~Order", "method=x", "method!=y",
		"class=com.example.Foo", "endpoint=GET /x", "user.tenant in a,b", "trace_id=None", "user.tenant!=Any",
		"endpoint=None", "retention_class not in short_clean", "request_id!=abc", "trace_id=~^4b",
	))
	require.Empty(t, detail)
	assert.Len(t, q.Filters, 15)
}

func TestParseCallsQueryRejectsWhatTheModelDoesNotHave(t *testing.T) {
	for text, message := range map[string]string{
		"duration=1s":                "does not take the operator =",
		"duration>soon":              "is not a duration",
		"service":                    "has no operator",
		"method in a,b":              "the field method does not take the operator in",
		"trace_id in a,b":            "the field trace_id does not take the operator in",
		"request_id!~a":              "the field request_id does not take the operator !~",
		"retention_class=~clean":     "the field retention_class does not take the operator =~",
		"error!=true":                "the field error does not take the operator !=",
		"class=None":                 "every call has a class, so None does not apply",
		"service!=Any":               "every call has a service, so Any does not apply",
		"retention_class!=sometimes": "is not a retention class",
		"user.tenant=~(":             "is not a valid regular expression",
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

func TestIdentityPrefixesCountBeforeBuilding(t *testing.T) {
	list := func(field string, value func(i int) string) string {
		values := make([]string, 200)
		for i := range values {
			values[i] = value(i)
		}
		return field + " in " + strings.Join(values, ",")
	}
	wide := conds(t,
		list("namespace", func(i int) string { return fmt.Sprintf("ns-%d", i) }),
		list("service", func(i int) string { return fmt.Sprintf("svc-%d", i) }),
		list("pod", func(i int) string { return fmt.Sprintf("pod-%d", i) }),
	)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, ok := wide.IdentityPrefixes()
	runtime.ReadMemStats(&after)
	assert.False(t, ok, "8,000,000 combinations fall back to row matching")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
		"the combinations are counted, not built")

	repeated := conds(t,
		list("namespace", func(int) string { return "a" }),
		list("service", func(int) string { return "a" }),
		list("pod", func(int) string { return "a" }),
	)
	got, ok := repeated.IdentityPrefixes()
	assert.True(t, ok, "a repeated value counts once")
	assert.Equal(t, []string{"a/a/a"}, got)

	// Contradicting pod conditions leave no pod, so no call matches. The
	// namespace and service lists must not be joined on the way.
	var namespaces, services []string
	for i := range 1000 {
		namespaces = append(namespaces, fmt.Sprintf("ns-%d", i))
		services = append(services, fmt.Sprintf("svc-%d", i))
	}
	contradicting := conds(t,
		"namespace in "+strings.Join(namespaces, ","),
		"service in "+strings.Join(services, ","),
		"pod=a", "pod=b",
	)
	runtime.ReadMemStats(&before)
	got, ok = contradicting.IdentityPrefixes()
	runtime.ReadMemStats(&after)
	assert.True(t, ok)
	assert.Empty(t, got, "no call matches")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
		"an empty pod list stops the join before it starts")
}

func TestCapabilitiesMatchTheModel(t *testing.T) {
	caps := Capabilities()
	assert.Equal(t, []Op{OpEq, OpNe, OpMatch, OpNotMatch, OpIn, OpNotIn}, caps.Fields["service"])
	assert.Equal(t, []Op{OpEq, OpNe, OpMatch, OpNotMatch, OpIn, OpNotIn}, caps.ParamOperators)
	assert.Equal(t, []Op{OpEq, OpNe, OpMatch, OpNotMatch}, caps.Fields["method"])
	assert.Equal(t, []string{ValueNone, ValueAny}, caps.SpecialValues)
	assert.Contains(t, caps.Fields, "class")
	assert.Contains(t, caps.Fields, "endpoint")
	assert.NotContains(t, caps.Fields, FieldPodTuple)
	for field, ops := range caps.Fields {
		for _, op := range ops {
			value := "x"
			if field == "duration" {
				value = "1s"
				if op == OpBetween {
					value = "1s..2s"
				}
			}
			if field == "error" {
				value = "true"
			}
			if field == "retention_class" {
				value = RetentionAnyError
			}
			text := Condition{field, op, []string{value}}.String()
			_, detail := ParseCallsQuery(window(text))
			assert.Empty(t, detail, text)
		}
	}
	caps.Fields["service"][0] = OpNe
	assert.Equal(t, OpEq, Capabilities().Fields["service"][0], "a caller cannot change the model")
}

func TestNegationViews(t *testing.T) {
	q := conds(t, "retention_class not in short_clean,normal_clean", "retention_class!=short_clean",
		"service!=billing", "user.tenant!~a", "namespace=prod")
	assert.Equal(t, []string{RetentionShortClean, RetentionNormalClean}, q.ExcludedRetentionClasses())
	assert.Equal(t, []Condition{{"namespace", OpEq, []string{"prod"}}}, q.WithoutNegations().Filters)
	assert.Len(t, q.Filters, 5, "the view does not change the query")
}
