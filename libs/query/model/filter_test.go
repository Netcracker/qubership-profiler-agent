package model

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cases below mirror apps/ui/src/filter/grammar.test.ts: the UI and the
// server must read and write the same text form (02 §2.3.3).

func TestParseConditionReadsTheUIForms(t *testing.T) {
	for _, tc := range []struct {
		text string
		want Condition
	}{
		{"pod=billing-1", Condition{"pod", OpEq, []string{"billing-1"}}},
		{"pod!=billing-1", Condition{"pod", OpNe, []string{"billing-1"}}},
		{"method=~Order", Condition{"method", OpMatch, []string{"Order"}}},
		{"method!~health", Condition{"method", OpNotMatch, []string{"health"}}},
		{"namespace in prod-orders,prod-payments", Condition{"namespace", OpIn, []string{"prod-orders", "prod-payments"}}},
		{"service not in billing", Condition{"service", OpNotIn, []string{"billing"}}},
		{"duration>100ms", Condition{"duration", OpGt, []string{"100ms"}}},
		{"duration>=2s", Condition{"duration", OpGe, []string{"2s"}}},
		{"duration<1.5s", Condition{"duration", OpLt, []string{"1.5s"}}},
		{"duration<=500ms", Condition{"duration", OpLe, []string{"500ms"}}},
		{"duration between 100ms..2s", Condition{"duration", OpBetween, []string{"100ms..2s"}}},
		{"trace_id=None", Condition{"trace_id", OpEq, []string{ValueNone}}},
		{"endpoint!=Any", Condition{"endpoint", OpNe, []string{ValueAny}}},
		{"user.tenant=acme", Condition{"user.tenant", OpEq, []string{"acme"}}},
		{"x-request-id=abc123", Condition{"x-request-id", OpEq, []string{"abc123"}}},
	} {
		got, err := ParseCondition(tc.text)
		require.NoError(t, err, tc.text)
		assert.Equal(t, tc.want, got, tc.text)
		assert.Equal(t, tc.text, got.String(), "the text form of %s", tc.text)
	}
}

func TestParseConditionAllowsSpacesAroundASymbolOperator(t *testing.T) {
	got, err := ParseCondition(" duration > 100ms ")
	require.NoError(t, err)
	assert.Equal(t, Condition{"duration", OpGt, []string{"100ms"}}, got)
}

func TestConditionEscapesACommaAndABackslashInAList(t *testing.T) {
	c := Condition{"user.tenant", OpIn, []string{"a,b", `c\d`}}
	assert.Equal(t, `user.tenant in a\,b,c\\d`, c.String())
	got, err := ParseCondition(c.String())
	require.NoError(t, err)
	assert.Equal(t, c, got)
}

func TestConditionKeepsAnEqualityValueStartingWithTildeApartFromARegexMatch(t *testing.T) {
	c := Condition{"pod", OpEq, []string{"~x"}}
	assert.Equal(t, "pod= ~x", c.String())
	got, err := ParseCondition("pod= ~x")
	require.NoError(t, err)
	assert.Equal(t, c, got)

	got, err = ParseCondition("pod=a=b")
	require.NoError(t, err)
	assert.Equal(t, Condition{"pod", OpEq, []string{"a=b"}}, got, "only the first operator splits")
}

func TestConditionRejections(t *testing.T) {
	for _, tc := range []struct{ text, message string }{
		{"", "does not start with a field name"},
		{"pod", "has no operator"},
		{"9pod=x", "is not a field name"},
		{"duration=100ms", "does not take the operator ="},
		{"error=yes", "is not a value of error"},
		{"duration>fast", "is not a duration"},
		{"duration between 2s", "is not a range"},
		{"pod=None", "does not apply"},
		{"method=Any", "does not apply"},
		{"error=None", "does not apply"},
		{"pod=", "has an empty value"},
		{"pod in a,,b", "has an empty value"},
		{`pod in a\`, "ends with a backslash"},
		{"retention_class=hot", "is not a retention class"},
		{"method=~(", "is not a valid regular expression"},
		{"class=Foo", "is not implemented"},
		{"endpoint=GET /x", "is not implemented"},
	} {
		c, err := ParseCondition(tc.text)
		if err == nil {
			_, err = compileCondition(c)
		}
		require.Error(t, err, tc.text)
		assert.Contains(t, err.Error(), tc.message, tc.text)
	}
}

// TestConditionRoundTrip checks that String and ParseCondition are inverses
// for any valid condition, the property the UI's grammar test checks.
func TestConditionRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const alphabet = "aZ0-.,\\=~!< /*"
	value := func() string {
		for {
			var b strings.Builder
			for n := 1 + rng.Intn(12); n > 0; n-- {
				b.WriteByte(alphabet[rng.Intn(len(alphabet))])
			}
			if v := strings.TrimSpace(b.String()); v != "" && v != ValueNone && v != ValueAny {
				return v
			}
		}
	}
	fieldsByKind := []string{"namespace", "service", "pod", "method", "trace_id", "user.tenant"}
	for i := 0; i < 2000; i++ {
		field := fieldsByKind[rng.Intn(len(fieldsByKind))]
		ops := fieldOf(field).ops
		op := ops[rng.Intn(len(ops))]
		c := Condition{Field: field, Op: op, Values: []string{value()}}
		if isListOp(op) {
			for n := rng.Intn(4); n > 0; n-- {
				c.Values = append(c.Values, value())
			}
		}
		got, err := ParseCondition(c.String())
		require.NoError(t, err, "%q", c.String())
		assert.Equal(t, c, got, "%q", c.String())
	}
}

func TestParseDuration(t *testing.T) {
	for text, want := range map[string]int64{
		"500ms": 500, "2s": 2000, "1.5s": 1500, "2": 2000, "0.4ms": 0, "0.5ms": 1, " 3 s ": 3000, "99999999s": 1 << 31,
	} {
		got, err := parseDuration(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, got, text)
	}
	for _, text := range []string{"", "fast", "-1s", "1m", "1..2", ">1s"} {
		_, err := parseDuration(text)
		assert.Error(t, err, text)
	}

	lo, hi, err := parseDurationRange("2s..100ms")
	require.NoError(t, err)
	assert.Equal(t, [2]int64{100, 2000}, [2]int64{lo, hi}, "a reversed range reads in ascending order")
}

func TestCanonicalConditionsIgnoreOrderAndSpelling(t *testing.T) {
	a := []Condition{
		{"service", OpIn, []string{"orders", "billing"}},
		{"duration", OpGe, []string{"1s"}},
		{"duration", OpBetween, []string{"2s..100ms"}},
	}
	b := []Condition{
		{"duration", OpBetween, []string{"100ms..2s"}},
		{"duration", OpGe, []string{"1000ms"}},
		{"service", OpIn, []string{"billing", "orders", "billing"}},
	}
	assert.Equal(t, CanonicalConditions(a), CanonicalConditions(b))
	assert.NotEqual(t, CanonicalConditions(a), CanonicalConditions(b[:2]))
}
