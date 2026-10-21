package model

import (
	"regexp"
	"slices"

	"github.com/pkg/errors"
)

type fieldKind int

const (
	kindParam    fieldKind = iota // a call param, by name or through an alias
	kindDuration                  // duration_ms
	kindString                    // a value every call has: identity, method, retention class
	kindBool                      // error_flag
	kindDerived                   // class and endpoint, derived from other columns
)

// fieldDef is one row of the field table of 02 §2.3.3.
type fieldDef struct {
	kind fieldKind
	ops  []Op
	// params are the call params a param field reads. An alias field
	// (trace_id, request_id) reads one or more; the values of all of them
	// count as the values of one param.
	params []string
	// value reads a kindString field from a row.
	value func(r *CallRow) string
}

var (
	durationOps = []Op{OpGt, OpGe, OpLt, OpLe, OpBetween}
	enumOps     = []Op{OpEq, OpNe, OpMatch, OpNotMatch, OpIn, OpNotIn}
	textOps     = []Op{OpEq, OpNe, OpMatch, OpNotMatch}
	idOps       = []Op{OpEq, OpNe, OpMatch}
)

func podTuple(r *CallRow) string {
	return r.PK.PodNamespace + "/" + r.PK.PodService + "/" + r.PK.PodName
}

// fields is the field table of 02 §2.3.3. A name outside it is a call param.
var fields = map[string]fieldDef{
	"duration":        {kind: kindDuration, ops: durationOps},
	"namespace":       {kind: kindString, ops: enumOps, value: func(r *CallRow) string { return r.PK.PodNamespace }},
	"service":         {kind: kindString, ops: enumOps, value: func(r *CallRow) string { return r.PK.PodService }},
	"pod":             {kind: kindString, ops: enumOps, value: func(r *CallRow) string { return r.PK.PodName }},
	"method":          {kind: kindString, ops: textOps, value: func(r *CallRow) string { return r.Method }},
	"class":           {kind: kindDerived, ops: enumOps},
	"endpoint":        {kind: kindDerived, ops: enumOps},
	"error":           {kind: kindBool, ops: []Op{OpEq}},
	"retention_class": {kind: kindString, ops: []Op{OpEq, OpNe, OpIn, OpNotIn}, value: func(r *CallRow) string { return r.RetentionClass }},
	"trace_id":        {kind: kindParam, ops: idOps, params: []string{"trace.id", "brave.trace_id", "X-B3-TraceId"}},
	"request_id":      {kind: kindParam, ops: idOps, params: []string{"x-request-id"}},
	FieldPodTuple:     {kind: kindString, ops: []Op{OpIn}, value: podTuple},
}

// fieldOf returns the definition of a field, or a call-param definition for a
// name outside the field table.
func fieldOf(name string) fieldDef {
	if def, ok := fields[name]; ok {
		return def
	}
	return fieldDef{kind: kindParam, ops: enumOps, params: []string{name}}
}

// rowPredicate reports whether a row meets one condition.
type rowPredicate func(r *CallRow) bool

// compileCondition checks a condition against the field table and returns its
// predicate. The error text is the 400 detail.
func compileCondition(c Condition) (rowPredicate, error) {
	def := fieldOf(c.Field)
	if c.Field != FieldPodTuple && !fieldName.MatchString(c.Field) {
		return nil, errors.Errorf("%q is not a field name", c.Field)
	}
	if !slices.Contains(def.ops, c.Op) {
		return nil, errors.Errorf("the field %s does not take the operator %s", c.Field, c.Op)
	}
	if len(c.Values) == 0 {
		return nil, errors.Errorf("%s needs a value", c)
	}
	if !isListOp(c.Op) && len(c.Values) > 1 {
		return nil, errors.Errorf("the operator %s takes one value", c.Op)
	}
	for _, v := range c.Values {
		if v == "" {
			return nil, errors.Errorf("%s has an empty value", c)
		}
		// None and Any are special with = and != only; with any other
		// operator they are literal text, so the legacy method=Any, which
		// becomes method=~Any, is still a substring search (02 §2.3.3).
		special := (v == ValueNone || v == ValueAny) && (c.Op == OpEq || c.Op == OpNe)
		if special && def.kind != kindParam {
			return nil, errors.Errorf("every call has a %s, so %s does not apply: %s", c.Field, v, c)
		}
	}
	switch def.kind {
	case kindDuration:
		return compileDuration(c)
	case kindBool:
		return compileError(c)
	case kindDerived:
		return nil, errors.Errorf("the field %s is not implemented", c.Field)
	case kindString:
		if c.Field == "retention_class" {
			for _, v := range c.Values {
				if !IsRetentionClass(v) {
					return nil, errors.Errorf("%q is not a retention class: %s", v, c)
				}
			}
		}
		value := def.value
		return compileText(c, func(r *CallRow) ([]string, bool) { return []string{value(r)}, true })
	default:
		names := def.params
		if len(names) == 1 {
			name := names[0]
			return compileText(c, func(r *CallRow) ([]string, bool) {
				vs := r.Params[name]
				return vs, len(vs) > 0
			})
		}
		return compileText(c, func(r *CallRow) ([]string, bool) {
			var vs []string
			for _, name := range names {
				vs = append(vs, r.Params[name]...)
			}
			return vs, len(vs) > 0
		})
	}
}

func compileDuration(c Condition) (rowPredicate, error) {
	if c.Op == OpBetween {
		lo, hi, err := parseDurationRange(c.Values[0])
		if err != nil {
			return nil, err
		}
		return func(r *CallRow) bool { d := int64(r.DurationMs); return d >= lo && d <= hi }, nil
	}
	ms, err := parseDuration(c.Values[0])
	if err != nil {
		return nil, err
	}
	switch c.Op {
	case OpGt:
		return func(r *CallRow) bool { return int64(r.DurationMs) > ms }, nil
	case OpGe:
		return func(r *CallRow) bool { return int64(r.DurationMs) >= ms }, nil
	case OpLt:
		return func(r *CallRow) bool { return int64(r.DurationMs) < ms }, nil
	default:
		return func(r *CallRow) bool { return int64(r.DurationMs) <= ms }, nil
	}
}

func compileError(c Condition) (rowPredicate, error) {
	switch c.Values[0] {
	case "true":
		return func(r *CallRow) bool { return r.ErrorFlag }, nil
	case "false":
		return func(r *CallRow) bool { return !r.ErrorFlag }, nil
	}
	return nil, errors.Errorf("%q is not a value of error; use true or false", c.Values[0])
}

// compileText builds the predicate of a field with text values. values returns
// the field's values on a row and whether the row has the field at all. A
// negative operator is the negation of its positive form, so it matches a row
// without the field (02 §2.3.3).
func compileText(c Condition, values func(r *CallRow) ([]string, bool)) (rowPredicate, error) {
	var positive rowPredicate
	switch c.Op {
	case OpEq, OpNe:
		want := c.Values[0]
		switch want {
		case ValueNone:
			positive = func(r *CallRow) bool { _, has := values(r); return !has }
		case ValueAny:
			positive = func(r *CallRow) bool { _, has := values(r); return has }
		default:
			positive = func(r *CallRow) bool { vs, _ := values(r); return slices.Contains(vs, want) }
		}
	case OpIn, OpNotIn:
		set := make(map[string]struct{}, len(c.Values))
		for _, v := range c.Values {
			set[v] = struct{}{}
		}
		positive = func(r *CallRow) bool {
			vs, _ := values(r)
			for _, v := range vs {
				if _, ok := set[v]; ok {
					return true
				}
			}
			return false
		}
	default: // OpMatch, OpNotMatch
		re, err := regexp.Compile(c.Values[0])
		if err != nil {
			return nil, errors.Errorf("%q is not a valid regular expression: %s", c.Values[0], c)
		}
		positive = func(r *CallRow) bool {
			vs, _ := values(r)
			return slices.ContainsFunc(vs, re.MatchString)
		}
	}
	if c.Op == OpNe || c.Op == OpNotIn || c.Op == OpNotMatch {
		return func(r *CallRow) bool { return !positive(r) }, nil
	}
	return positive, nil
}

// Matcher applies a query's row-level predicates: the ts window and every
// condition. Build it once per query with CallsQuery.Matcher; matching a row
// compiles nothing.
type Matcher struct {
	fromMs, toMs int64
	preds        []rowPredicate
}

// Matcher compiles the query's conditions. The error text is the 400 detail.
func (q CallsQuery) Matcher() (*Matcher, error) {
	m := &Matcher{fromMs: q.FromMs, toMs: q.ToMs}
	for _, c := range q.Filters {
		p, err := compileCondition(c)
		if err != nil {
			return nil, err
		}
		m.preds = append(m.preds, p)
	}
	return m, nil
}

// Match reports whether a row is in the window and meets every condition.
func (m *Matcher) Match(r *CallRow) bool {
	if r.TsMs < m.fromMs || r.TsMs >= m.toMs {
		return false
	}
	for _, p := range m.preds {
		if !p(r) {
			return false
		}
	}
	return true
}
