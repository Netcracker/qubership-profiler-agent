package model

import (
	"slices"
)

// The filter model is implemented in full (02 §2.3.4, stage B): the field
// table of match.go is the gate, and Capabilities reports it.

// StatsGroupings are the named /stats groupings this build implements
// (02 §2.9). Besides them, group_by takes the name of any call param.
var StatsGroupings = []string{
	"time", "method", "namespace", "service", "pod", "class", "endpoint", "error", "retention_class",
	"trace_id", "request_id", GroupByParamName,
}

// GroupByParamName groups the calls by the names of the params they carry.
const GroupByParamName = "param_name"

// IsSingleValued reports whether a call has at most one value of a field, so
// the /stats rows of a grouping by it are disjoint and the counts of any set
// of rows add up to the calls in them. A call param, trace_id, request_id,
// and param_name can have several values per call; a name outside the field
// table is a param.
func IsSingleValued(name string) bool {
	def, ok := fields[name]
	return ok && def.kind != kindParam
}

// IsStatsGrouping reports whether group_by names a grouping: a named one or
// a call param. `duration` is a field without text values, so nothing groups
// by it.
func IsStatsGrouping(name string) bool {
	if slices.Contains(StatsGroupings, name) {
		return true
	}
	_, field := fields[name]
	return !field && fieldName.MatchString(name)
}

// FilterCapabilities is the `filter_capabilities` member of /api/v1/config
// (02 §2.3.4): what this build implements of the filter model, `order`, and
// `/stats`.
type FilterCapabilities struct {
	Fields         map[string][]Op `json:"fields"`
	ParamOperators []Op            `json:"param_operators"`
	SpecialValues  []string        `json:"special_values"`
	Orders         []string        `json:"orders"`
	GroupBy        []string        `json:"group_by"`
	// GroupByParams reports that group_by also takes the name of any call
	// param, which a list cannot enumerate.
	GroupByParams bool `json:"group_by_params"`
}

// Capabilities reports what this build implements.
func Capabilities() FilterCapabilities {
	out := make(map[string][]Op, len(fields))
	for name, def := range fields {
		if name != FieldPodTuple {
			out[name] = slices.Clone(def.ops)
		}
	}
	return FilterCapabilities{
		Fields:         out,
		ParamOperators: slices.Clone(fieldOf("").ops),
		SpecialValues:  []string{ValueNone, ValueAny},
		Orders:         orderNames(),
		GroupBy:        slices.Clone(StatsGroupings),
		GroupByParams:  true,
	}
}

func orderNames() []string {
	out := make([]string, len(ImplementedOrders))
	for i, o := range ImplementedOrders {
		out[i] = o.String()
	}
	return out
}
