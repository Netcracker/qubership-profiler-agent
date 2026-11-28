package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapToJsonString(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		actual := MapToJsonString(map[string]string{"a": "b"})
		assert.Equal(t, "{\"a\":\"b\"}", actual)

		actual = MapToJsonString(map[string]int{"a": 12, "": -3})
		assert.Equal(t, "{\"\":-3,\"a\":12}", actual)

		actual = MapToJsonString(123)
		assert.Equal(t, "123", actual)

		actual = MapToJsonString(123.34)
		assert.Equal(t, "123.34", actual)

		actual = MapToJsonString("123")
		assert.Equal(t, "\"123\"", actual)

		actual = MapToJsonString[any](nil)
		assert.Equal(t, "null", actual)
	})

	t.Run("invalid", func(t *testing.T) {
		v := map[string]interface{}{"text": "as"}
		v["self"] = v

		// JSON marshal does not handle cyclic data structures
		actual := MapToJsonString(v)
		assert.Equal(t, "", actual)
	})
}

func TestRef(t *testing.T) {
	t.Run("ref", func(t *testing.T) {
		assert.NotNil(t, *Ref("text"))
		assert.Equal(t, "text", *Ref("text"))
		assert.Equal(t, 123, *Ref(123))
	})
}
