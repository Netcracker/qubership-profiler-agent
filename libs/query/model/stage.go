package model

import (
	"slices"
)

// The filter model is implemented in full (02 §2.3.4, stage B): the field
// table of match.go is the gate, and Capabilities reports it.

// StatsGroupings are the /stats groupings this build implements
// (02 §2.9, stage A).
var StatsGroupings = []string{"time", "method", "namespace", "service", "pod"}

// FilterCapabilities is the `filter_capabilities` member of /api/v1/config
// (02 §2.3.4): what this build implements of the filter model, `order`, and
// `/stats`.
type FilterCapabilities struct {
	Fields         map[string][]Op `json:"fields"`
	ParamOperators []Op            `json:"param_operators"`
	SpecialValues  []string        `json:"special_values"`
	Orders         []string        `json:"orders"`
	GroupBy        []string        `json:"group_by"`
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
	}
}

func orderNames() []string {
	out := make([]string, len(ImplementedOrders))
	for i, o := range ImplementedOrders {
		out[i] = o.String()
	}
	return out
}
