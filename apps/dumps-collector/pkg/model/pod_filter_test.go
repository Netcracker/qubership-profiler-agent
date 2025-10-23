//go:build unit

package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPodFilter(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		podFilterJson := "{}"
		var podFilter PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter)
		require.NoError(t, err)
		assert.Equal(t, "", podFilter.SQLQuery())
	})

	t.Run("wrong json", func(t *testing.T) {
		podFilterJson := `{"}`
		var podFilter PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter)
		assert.ErrorContains(t, err, "unexpected end of JSON input")
	})

	t.Run("missed or empty lValue in comparator json", func(t *testing.T) {
		podFilterJson := `{ 
			"comparator": "=", 
			"rValues": [
				{"word": "ns-0"}
			]
		}`
		var podFilter1 PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter1)
		assert.NoError(t, err)
		err = podFilter1.Validate()
		assert.ErrorContains(t, err, "lValue property error: not specified")

		podFilterJson = `{
		    "lValue": {}, 
			"comparator": "=", 
			"rValues": [
				{"word": "ns-0"}
			]
		}`
		var podFilter2 PodFilter = &PodFilterContainer{}
		err = json.Unmarshal([]byte(podFilterJson), podFilter2)
		assert.NoError(t, err)
		err = podFilter2.Validate()
		assert.ErrorContains(t, err, "lValue property error: missed word property")
	})

	t.Run("missed or unsupported comparator in comparator json", func(t *testing.T) {
		podFilterJson := `{
			    "lValue": {"word": "namespace"}, 
				"rValues": [
					{"word": "ns-0"}
				]
			}`
		var podFilter1 PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter1)
		assert.NoError(t, err)
		err = podFilter1.Validate()
		assert.ErrorContains(t, err, "comparator property error: not specified")

		podFilterJson = `{
				"lValue": {"word": "namespace"}, 
				"comparator": "!=", 
				"rValues": [
					{"word": "ns-0"}
				]
			}`
		var podFilter2 PodFilter = &PodFilterContainer{}
		err = json.Unmarshal([]byte(podFilterJson), podFilter2)
		assert.NoError(t, err)
		err = podFilter2.Validate()
		assert.ErrorContains(t, err, "comparator property error: unsupported value \"!=\"")
	})

	t.Run("missed empty rValue in comparator json", func(t *testing.T) {
		podFilterJson := `{
			    "lValue": {"word": "namespace"}, 
				"comparator": "="
			}`
		var podFilter1 PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter1)
		assert.NoError(t, err)
		err = podFilter1.Validate()
		assert.NoError(t, err)

		podFilterJson = `{
			"lValue": {"word": "namespace"}, 
			"comparator": "=",
			"rValues": [
				{}
			]
		}`
		var podFilter2 PodFilter = &PodFilterContainer{}
		err = json.Unmarshal([]byte(podFilterJson), podFilter2)
		assert.NoError(t, err)
		err = podFilter2.Validate()
		assert.ErrorContains(t, err, "rValues[0] property error: missed word property")
	})

	t.Run("valid comparator json", func(t *testing.T) {
		podFilterJson := `{
			"lValue":{"word":"namespace"},
			"comparator":"=",
			"rValues":[
				{"word":"profiler"}
			]
		}`
		var podFilter PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter)
		require.NoError(t, err)

		ns0PodFilter := toContainer(NewPodFilterComparator("namespace", ComparatorEqual, "profiler"))
		assert.Equal(t, ns0PodFilter, podFilter)

		expectedSQLQuery := `namespace IN ('profiler')`
		assert.Equal(t, expectedSQLQuery, podFilter.SQLQuery())
	})

	t.Run("valid multivalues comparator json", func(t *testing.T) {
		podFilterJson := `{
			"lValue": {"word": "namespace"}, 
			"comparator": "=", 
			"rValues": [
				{"word": "profiler1"}, 
				{"word": "profiler2"}, 
				{"word": "profiler3"}
			]
		}`
		var podFilter PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter)
		require.NoError(t, err)

		ns0PodFilter := toContainer(
			NewPodFilterComparator("namespace", ComparatorEqual,
				"profiler1", "profiler2", "profiler3"))
		assert.Equal(t, ns0PodFilter, podFilter)

		expectedSQLQuery := `namespace IN ('profiler1','profiler2','profiler3')`
		assert.Equal(t, expectedSQLQuery, podFilter.SQLQuery())
	})

	t.Run("missed or unsupported operation in condition json", func(t *testing.T) {
		podFilterJson := `{
			"conditions": []
		}`
		var podFilter1 PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter1)
		assert.NoError(t, err)

		err = podFilter1.Validate()
		assert.Errorf(t, err, "operation property error: not specified")

		podFilterJson = `{
		    "operation": "some",
			"conditions": []
		}`
		var podFilter2 PodFilter = &PodFilterContainer{}
		err = json.Unmarshal([]byte(podFilterJson), podFilter2)
		assert.NoError(t, err)

		err = podFilter1.Validate()
		assert.Errorf(t, err, "operation property error: unsupported value \"some\"")
	})

	t.Run("valid condition json", func(t *testing.T) {
		podFilterJson := `{
			"operation":"and",
			"conditions":[{
				"lValue":{"word":"service_name"},
				"comparator":"=",
				"rValues":[
					{"word":"esc-collector-service"}
				]
			},{
				"lValue":{"word":"namespace"},
				"comparator":"=",
				"rValues":[
					{"word":"profiler"}
				]
			}]
		}`
		var podFilter PodFilter = &PodFilterContainer{}
		err := json.Unmarshal([]byte(podFilterJson), podFilter)
		require.NoError(t, err)

		ns0PodFilter := toContainer(NewPodFilterСondition(
			OperationAnd,
			NewPodFilterComparator("service_name", ComparatorEqual, "esc-collector-service"),
			NewPodFilterComparator("namespace", ComparatorEqual, "profiler"),
		))
		assert.Equal(t, ns0PodFilter, podFilter)

		expectedSQLQuery := `(service_name IN ('esc-collector-service')) and (namespace IN ('profiler'))`
		assert.Equal(t, expectedSQLQuery, podFilter.SQLQuery())
	})
}
