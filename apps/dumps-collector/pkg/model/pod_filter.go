package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

type PodFilter interface {
	SQLQuery() string
	Validate() error
}

type PodFilterContainer struct {
	child PodFilter
}

// TODO check SQL injections
func (p *PodFilterContainer) SQLQuery() string {
	return p.child.SQLQuery()
}

func (p *PodFilterContainer) Validate() error {
	return p.child.Validate()
}

func (p *PodFilterContainer) UnmarshalJSON(b []byte) error {
	data := make(map[string]json.RawMessage)
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	if len(data) == 0 {
		p.child = &EmptyPodFilter{}
		return nil
	}
	if _, found := data["operation"]; !found {
		p.child = &PodFilterComparator{}
	} else {
		p.child = &PodFilterCondition{}
	}
	if err := json.Unmarshal(b, &p.child); err != nil {
		return err
	}
	return nil
}

type PodFilterValue struct {
	Word *string `json:"word"`
}

func (v PodFilterValue) String() string {
	return *v.Word
}

func (p *PodFilterValue) Validate() error {
	if p == nil {
		return fmt.Errorf("not specified")
	}
	if p.Word == nil {
		return fmt.Errorf("missed word property")
	}
	return nil
}

type ComparatorType string

const (
	ComparatorEqual = ComparatorType("=")
)

func (c *ComparatorType) Validate() error {
	if c == nil {
		return fmt.Errorf("not specified")
	}
	if *c != ComparatorEqual {
		return fmt.Errorf("unsupported value \"%s\"", *c)
	}
	return nil
}

type PodFilterComparator struct {
	LValue     *PodFilterValue  `json:"lValue"`
	Comparator *ComparatorType  `json:"comparator"`
	RValues    []PodFilterValue `json:"rValues"`
}

func (c *PodFilterComparator) SQLQuery() string {
	switch *c.Comparator {
	case ComparatorEqual:
		rValuesWords := make([]string, len(c.RValues))
		for i, value := range c.RValues {
			rValuesWords[i] = fmt.Sprintf("'%s'", *value.Word)
		}
		return fmt.Sprintf("%s IN (%s)", *c.LValue.Word, strings.Join(rValuesWords, ","))
	default:
		return "Not implemented"
	}
}

func (c *PodFilterComparator) Validate() error {
	if err := c.LValue.Validate(); err != nil {
		return fmt.Errorf("lValue property error: %w", err)
	}
	if err := c.Comparator.Validate(); err != nil {
		return fmt.Errorf("comparator property error: %w", err)
	}
	for i, rValue := range c.RValues {
		if err := rValue.Validate(); err != nil {
			return fmt.Errorf("rValues[%d] property error: %w", i, err)
		}
	}
	return nil
}

type OperationType string

const (
	OperationAnd = OperationType("and")
	OperationOr  = OperationType("or")
)

func (c *OperationType) Validate() error {
	if c == nil {
		return fmt.Errorf("not specified")
	}
	if *c != OperationAnd && *c != OperationOr {
		return fmt.Errorf("unsupported value \"%s\"", *c)
	}
	return nil
}

type PodFilterCondition struct {
	Operation  OperationType        `json:"operation"`
	Conditions []PodFilterContainer `json:"conditions"`
}

func (c *PodFilterCondition) SQLQuery() string {
	conditionQueries := make([]string, len(c.Conditions))
	for i, condition := range c.Conditions {
		conditionQueries[i] = fmt.Sprintf("(%s)", condition.SQLQuery())
	}
	return strings.Join(conditionQueries, fmt.Sprintf(" %s ", c.Operation))
}

func (c *PodFilterCondition) Validate() error {
	if err := c.Operation.Validate(); err != nil {
		return fmt.Errorf("operation property error: %w", err)
	}
	for i, condition := range c.Conditions {
		if err := condition.Validate(); err != nil {
			return fmt.Errorf("conditions[%d] property error: %w", i, err)
		}
	}
	return nil
}

type EmptyPodFilter struct{}

func (f EmptyPodFilter) SQLQuery() string {
	return ""
}

func (f EmptyPodFilter) Validate() error {
	return nil
}

func NewPodFilterComparator(name string, comparator ComparatorType, values ...string) PodFilter {
	res := &PodFilterComparator{
		LValue: &PodFilterValue{
			Word: &name,
		},
		Comparator: &comparator,
		RValues:    make([]PodFilterValue, len(values)),
	}
	for i, value := range values {
		res.RValues[i] = PodFilterValue{Word: &value}
	}
	return res
}

func NewPodFilterСondition(operation OperationType, conditions ...PodFilter) PodFilter {
	res := &PodFilterCondition{
		Operation:  operation,
		Conditions: make([]PodFilterContainer, len(conditions)),
	}
	for i, condition := range conditions {
		res.Conditions[i].child = condition
	}
	return res
}

func toContainer(childFilter PodFilter) PodFilter {
	return &PodFilterContainer{
		child: childFilter,
	}
}
