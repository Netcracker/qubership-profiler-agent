package model

import (
	"slices"

	"github.com/pkg/errors"
)

// stageA is the part of the filter model the `filter` parameter accepts in
// this build (02 §2.3.4): each field with its implemented operators.
// Matcher implements more than this, because the legacy `pod` parameter
// needs an internal pod-tuple field; the public gate is this table.
var stageA = map[string][]Op{
	"duration":        durationOps,
	"namespace":       {OpEq, OpIn},
	"service":         {OpEq, OpIn},
	"pod":             {OpEq, OpIn},
	"retention_class": {OpEq, OpIn},
	"method":          {OpMatch},
	"error":           {OpEq},
	"trace_id":        {OpEq},
	"request_id":      {OpEq},
}

// StatsGroupings are the /stats groupings this build implements
// (02 §2.9, stage A).
var StatsGroupings = []string{"time", "method", "namespace", "service", "pod"}

// stageAParamOps are the operators the `filter` parameter accepts on a call
// param.
var stageAParamOps = []Op{OpEq}

// checkImplemented rejects a valid condition that the `filter` parameter does
// not accept yet. The error text is the 400 detail of 02 §2.3.4.
func checkImplemented(c Condition) error {
	ops, known := stageA[c.Field]
	if !known {
		if _, field := fields[c.Field]; field {
			return errors.Errorf("the field %s is not implemented", c.Field)
		}
		ops = stageAParamOps
	}
	if !slices.Contains(ops, c.Op) {
		return errors.Errorf("%s on %s is not implemented", c.Op, c.Field)
	}
	// None and Any are special values with = and != only; with any other
	// operator they are text, which stage A takes (02 §2.3.3).
	if c.Op != OpEq && c.Op != OpNe {
		return nil
	}
	for _, v := range c.Values {
		if v == ValueNone || v == ValueAny {
			return errors.Errorf("the value %s is not implemented", v)
		}
	}
	return nil
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
}

// Capabilities reports the stage this build implements.
func Capabilities() FilterCapabilities {
	fields := make(map[string][]Op, len(stageA))
	for name, ops := range stageA {
		fields[name] = slices.Clone(ops)
	}
	return FilterCapabilities{
		Fields:         fields,
		ParamOperators: slices.Clone(stageAParamOps),
		SpecialValues:  []string{},
		Orders:         []string{"start_desc"},
		GroupBy:        []string{},
	}
}
