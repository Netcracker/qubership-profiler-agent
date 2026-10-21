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
// follows the first condition's order. The result holds no duplicates.
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
				out = distinct(c.Values)
			}
			continue
		}
		out = slices.DeleteFunc(out, func(v string) bool { return !slices.Contains(c.Values, v) })
	}
	return out, found
}

// maxIdentityPrefixes caps the pushed-down prefix list; a wider combination
// is left to Matcher rather than rendered as hundreds of LIKE terms.
const maxIdentityPrefixes = 64

// IdentityFilter reports whether a condition selects calls by identity with
// `=` or `in`: a pod tuple, a namespace, a service, or a pod. The guard
// counts such a query as pod-filtered (02 §2.3.2).
func (q CallsQuery) IdentityFilter() bool {
	return slices.ContainsFunc(q.Filters, func(c Condition) bool {
		switch c.Field {
		case FieldPodTuple, "namespace", "service", "pod":
			return c.Op == OpEq || c.Op == OpIn
		}
		return false
	})
}

// IdentityPrefixes returns "<namespace>", "<namespace>/<service>", or
// "<namespace>/<service>/<pod>" prefixes that every call allowed by the
// namespace, service, and pod conditions starts with. ok is false when the
// conditions cannot be written as prefixes: no namespace condition, or more
// than maxIdentityPrefixes combinations. A store pushes the prefixes down as
// a filter on its "<namespace>/<service>/<pod>/..." key; pod tuples go
// through PodTuples instead.
func (q CallsQuery) IdentityPrefixes() (prefixes []string, ok bool) {
	namespaces, ok := q.intersectValues("namespace", nil)
	if !ok {
		return nil, false
	}
	services, hasServices := q.intersectValues("service", nil)
	pods, hasPods := q.intersectValues("pod", nil)
	// Count the combinations before building them: three lists of a few
	// hundred values each would otherwise allocate millions of strings only
	// to be dropped.
	count := len(namespaces)
	if hasServices {
		count = cappedProduct(count, len(services))
		if hasPods {
			count = cappedProduct(count, len(pods))
		}
	}
	if count > maxIdentityPrefixes {
		return nil, false
	}
	prefixes = namespaces
	if hasServices {
		prefixes = joinEach(prefixes, services)
		if hasPods {
			prefixes = joinEach(prefixes, pods)
		}
	}
	return prefixes, true
}

// cappedProduct returns a*b for non-negative a and b, or
// maxIdentityPrefixes+1 when the product exceeds maxIdentityPrefixes, so a
// chain of products never overflows.
func cappedProduct(a, b int) int {
	if b != 0 && a > maxIdentityPrefixes/b {
		return maxIdentityPrefixes + 1
	}
	return a * b
}

// distinct returns the values without repeats, in the order each first
// appears.
func distinct(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, dup := seen[v]; !dup {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

func joinEach(heads, tails []string) []string {
	out := make([]string, 0, len(heads)*len(tails))
	for _, h := range heads {
		for _, t := range tails {
			out = append(out, h+"/"+t)
		}
	}
	return out
}
