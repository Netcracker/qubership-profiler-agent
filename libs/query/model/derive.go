package model

import (
	"math"
	"regexp"
	"slices"
	"strconv"
)

// The conditions the legacy /calls parameters stand for (02 §2.3.3).

// PodTuplesCondition is the legacy `pod` parameter: the call's
// "<namespace>/<service>/<pod>" is one of tuples.
func PodTuplesCondition(tuples ...string) Condition {
	return Condition{Field: FieldPodTuple, Op: OpIn, Values: tuples}
}

// MethodContainsCondition is the legacy `method` parameter, a substring match.
func MethodContainsCondition(text string) Condition {
	return Condition{Field: "method", Op: OpMatch, Values: []string{regexp.QuoteMeta(text)}}
}

// DurationCondition is `duration <op> <ms>ms`; the legacy duration_min_ms and
// duration_max_ms are OpGe and OpLe.
func DurationCondition(op Op, ms int64) Condition {
	return Condition{Field: "duration", Op: op, Values: []string{strconv.FormatInt(ms, 10) + "ms"}}
}

// ErrorCondition is `error=true` (the legacy error_only) or `error=false`.
func ErrorCondition(flag bool) Condition {
	return Condition{Field: "error", Op: OpEq, Values: []string{strconv.FormatBool(flag)}}
}

// RetentionClassCondition is the legacy retention_class parameter.
func RetentionClassCondition(classes ...string) Condition {
	return Condition{Field: "retention_class", Op: OpIn, Values: classes}
}

// The views below narrow the conditions to what a store can push down or what
// discovery and the guard prune by. Each view is a superset of the matching
// rows; Matcher stays the authority. A condition that does not compile is
// ignored here, because Matcher rejects the query before any view is used.

// DurationBounds returns the inclusive range every duration condition allows:
// minMs is 0 and maxMs is math.MaxInt64 when that side is unbounded.
func (q CallsQuery) DurationBounds() (minMs, maxMs int64) {
	minMs, maxMs = 0, math.MaxInt64
	for _, c := range q.Filters {
		if c.Field != "duration" || len(c.Values) != 1 {
			continue
		}
		lo, hi := int64(0), int64(math.MaxInt64)
		if c.Op == OpBetween {
			a, b, err := parseDurationRange(c.Values[0])
			if err != nil {
				continue
			}
			lo, hi = a, b
		} else {
			ms, err := parseDuration(c.Values[0])
			if err != nil {
				continue
			}
			switch c.Op {
			case OpGt:
				lo = ms + 1
			case OpGe:
				lo = ms
			case OpLt:
				hi = ms - 1
			case OpLe:
				hi = ms
			}
		}
		minMs, maxMs = max(minMs, lo), min(maxMs, hi)
	}
	return minMs, maxMs
}

// ErrorRequired reports whether a condition admits only failed calls.
func (q CallsQuery) ErrorRequired() bool {
	return slices.ContainsFunc(q.Filters, func(c Condition) bool {
		return c.Field == "error" && c.Op == OpEq && len(c.Values) == 1 && c.Values[0] == "true"
	})
}

// RetentionClassSet returns the classes every `=` and `in` retention_class
// condition allows, in the order of RetentionClasses. ok is false when no
// such condition exists; an empty set with ok means no class is allowed.
func (q CallsQuery) RetentionClassSet() (classes []string, ok bool) {
	return q.intersectValues("retention_class", RetentionClasses)
}

// PodTuples returns the "<namespace>/<service>/<pod>" tuples every pod-tuple
// condition allows, in the order the first condition lists them. ok is false
// when the query has no pod-tuple condition.
func (q CallsQuery) PodTuples() (tuples []string, ok bool) {
	return q.intersectValues(FieldPodTuple, nil)
}

// intersectValues intersects the values of the field's `=` and `in`
// conditions. With a universe, the result follows its order; without one, it
// follows the first condition's order.
func (q CallsQuery) intersectValues(field string, universe []string) ([]string, bool) {
	var out []string
	found := false
	for _, c := range q.Filters {
		if c.Field != field || (c.Op != OpEq && c.Op != OpIn) {
			continue
		}
		if !found {
			found = true
			if universe != nil {
				out = slices.DeleteFunc(slices.Clone(universe), func(v string) bool { return !slices.Contains(c.Values, v) })
			} else {
				out = slices.Clone(c.Values)
			}
			continue
		}
		out = slices.DeleteFunc(out, func(v string) bool { return !slices.Contains(c.Values, v) })
	}
	return out, found
}
