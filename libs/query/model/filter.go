package model

import (
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Op is a filter operator of the read contract's filter model (02 §2.3.3).
type Op string

const (
	OpEq       Op = "="
	OpNe       Op = "!="
	OpMatch    Op = "=~"
	OpNotMatch Op = "!~"
	OpGt       Op = ">"
	OpGe       Op = ">="
	OpLt       Op = "<"
	OpLe       Op = "<="
	OpIn       Op = "in"
	OpNotIn    Op = "not in"
	OpBetween  Op = "between"
)

// The special values of an optional field (02 §2.3.3).
const (
	ValueNone = "None"
	ValueAny  = "Any"
)

// FieldPodTuple is the internal field the legacy `pod` parameter becomes: the
// call's "<namespace>/<service>/<pod>" is one of the values. Its name fails
// the field-name rule, so a client cannot write it as a `filter` value.
const FieldPodTuple = "@pod"

// Condition is one filter condition: a field, an operator, and a value. Every
// operator takes exactly one value except `in` and `not in`, which take a
// list. A duration value is kept as written (`500ms`, `1.5s`); a `between`
// value is the range text (`100ms..2s`). The struct rides inside the cursor.
type Condition struct {
	Field  string   `json:"field"`
	Op     Op       `json:"op"`
	Values []string `json:"values"`
}

// symbolOps are the operators written without spaces, longest first, so `>=`
// is not read as `>` followed by `=`.
var symbolOps = []Op{OpMatch, OpNotMatch, OpNe, OpGe, OpLe, OpEq, OpGt, OpLt}

// conditionHead reads the field name and a word operator with its spaces. It
// mirrors HEAD in apps/ui/src/filter/grammar.ts; the two must accept the same
// text.
var conditionHead = regexp.MustCompile(`^([^\s=!~<>]+)(?:\s+(not in|in|between)\s+|\s*)`)

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)

func isListOp(op Op) bool {
	return op == OpIn || op == OpNotIn
}

// ParseCondition reads the text form of 02 §2.3.3. It checks the syntax only;
// compileCondition checks the field, the operator, and the value.
func ParseCondition(text string) (Condition, error) {
	trimmed := strings.TrimSpace(text)
	head := conditionHead.FindStringSubmatch(trimmed)
	if head == nil {
		return Condition{}, errors.Errorf("%q does not start with a field name", trimmed)
	}
	field := head[1]
	if !fieldName.MatchString(field) {
		return Condition{}, errors.Errorf("%q is not a field name", field)
	}
	rest := trimmed[len(head[0]):]
	switch word := Op(head[2]); word {
	case OpBetween:
		return Condition{Field: field, Op: word, Values: []string{rest}}, nil
	case OpIn, OpNotIn:
		values, err := splitListValues(rest)
		if err != nil {
			return Condition{}, errors.Wrapf(err, "%q", trimmed)
		}
		return Condition{Field: field, Op: word, Values: values}, nil
	}
	for _, op := range symbolOps {
		if strings.HasPrefix(rest, string(op)) {
			value := strings.TrimSpace(rest[len(op):])
			return Condition{Field: field, Op: op, Values: []string{value}}, nil
		}
	}
	return Condition{}, errors.Errorf("%q has no operator after the field %s", trimmed, field)
}

// splitListValues splits on unescaped commas and removes the escapes.
func splitListValues(text string) ([]string, error) {
	var out []string
	var current strings.Builder
	for i := 0; i < len(text); i++ {
		switch ch := text[i]; ch {
		case '\\':
			if i+1 == len(text) {
				return nil, errors.New("the value list ends with a backslash")
			}
			current.WriteByte(text[i+1])
			i++
		case ',':
			out = append(out, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	return append(out, strings.TrimSpace(current.String())), nil
}

var listEscaper = strings.NewReplacer(`\`, `\\`, `,`, `\,`)

// String returns the text form of the condition, the inverse of
// ParseCondition.
func (c Condition) String() string {
	if isListOp(c.Op) {
		escaped := make([]string, len(c.Values))
		for i, v := range c.Values {
			escaped[i] = listEscaper.Replace(v)
		}
		return c.Field + " " + string(c.Op) + " " + strings.Join(escaped, ",")
	}
	value := ""
	if len(c.Values) > 0 {
		value = c.Values[0]
	}
	if c.Op == OpBetween {
		return c.Field + " between " + value
	}
	// `pod=~x` reads as a regex match, so a value that starts with an operator
	// character keeps a space before it.
	if strings.HasPrefix(value, "=") || strings.HasPrefix(value, "~") {
		return c.Field + string(c.Op) + " " + value
	}
	return c.Field + string(c.Op) + value
}

// canonical returns the text form with the value spelling normalized, so two
// conditions that mean the same thing compare equal: durations in whole
// milliseconds, a range in ascending order, a list sorted and without
// duplicates, and a one-value list as `=` or `!=`. A value that does not
// parse is kept as written.
func (c Condition) canonical() string {
	n := Condition{Field: c.Field, Op: c.Op, Values: append([]string(nil), c.Values...)}
	if fieldOf(c.Field).kind == kindDuration && len(n.Values) == 1 {
		if c.Op == OpBetween {
			if lo, hi, err := parseDurationRange(n.Values[0]); err == nil {
				n.Values[0] = strconv.FormatInt(lo, 10) + "ms.." + strconv.FormatInt(hi, 10) + "ms"
			}
		} else if ms, err := parseDuration(n.Values[0]); err == nil {
			n.Values[0] = strconv.FormatInt(ms, 10) + "ms"
		}
	}
	if isListOp(c.Op) {
		sort.Strings(n.Values)
		n.Values = dedupSorted(n.Values)
		if len(n.Values) == 1 {
			n.Op = map[Op]Op{OpIn: OpEq, OpNotIn: OpNe}[c.Op]
		}
	}
	return n.String()
}

func dedupSorted(vs []string) []string {
	out := vs[:0]
	for i, v := range vs {
		if i == 0 || v != vs[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// CanonicalConditions returns the canonical text of each condition, sorted,
// so two condition lists compare equal when they hold the same conditions in
// any order (02 §2.3.1).
func CanonicalConditions(cs []Condition) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.canonical()
	}
	sort.Strings(out)
	return dedupSorted(out)
}

var (
	durationValue = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s)?$`)
	durationRange = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s)?\.{2,}(\d+(?:\.\d+)?)(ms|s)?$`)
)

// parseDuration reads one duration value: a number with the unit `ms` or `s`,
// where a bare number is seconds, as the UI's duration filter reads it.
// Whitespace is ignored and the result is rounded to whole milliseconds.
func parseDuration(text string) (int64, error) {
	m := durationValue.FindStringSubmatch(stripSpace(text))
	if m == nil {
		return 0, errors.Errorf("%q is not a duration such as 500ms or 2s", text)
	}
	return durationMs(m[1], m[2])
}

// parseDurationRange reads `<a>..<b>` and returns the bounds in ascending
// order: the UI accepts a reversed range, so the server does too.
func parseDurationRange(text string) (lo, hi int64, err error) {
	m := durationRange.FindStringSubmatch(stripSpace(text))
	if m == nil {
		return 0, 0, errors.Errorf("%q is not a range such as 100ms..2s", text)
	}
	if lo, err = durationMs(m[1], m[2]); err != nil {
		return 0, 0, err
	}
	if hi, err = durationMs(m[3], m[4]); err != nil {
		return 0, 0, err
	}
	if lo > hi {
		lo, hi = hi, lo
	}
	return lo, hi, nil
}

func durationMs(number, unit string) (int64, error) {
	v, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, errors.Errorf("%q is not a number", number)
	}
	if unit != "ms" {
		v *= 1000
	}
	v = math.Round(v)
	// A call duration is an int32 of milliseconds; anything past it matches
	// the same calls as the int32 maximum, so clamping changes no answer.
	if v > math.MaxInt32 {
		v = math.MaxInt32
	}
	return int64(v), nil
}

func stripSpace(s string) string {
	return strings.Join(strings.Fields(s), "")
}
