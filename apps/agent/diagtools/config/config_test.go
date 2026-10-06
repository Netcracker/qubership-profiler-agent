package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_filterProperties(t *testing.T) {
	t.Run("filter list", func(t *testing.T) {
		a := []string{"asd", "bsd", "cde"}
		res := filterProperties(a, "a", "b", "asd")
		assert.Equal(t, []string{"asd"}, res)

		res = filterProperties(a, "a", "b", "asd", "asd", "asd")
		assert.Equal(t, []string{"asd"}, res)

		res = filterProperties(a, "a", "b", "asd", "cde", "asd")
		assert.Equal(t, []string{"asd", "cde"}, res)

		res = filterProperties(a, "a", "b")
		assert.Equal(t, 0, len(res))
	})
}
