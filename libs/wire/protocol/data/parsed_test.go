package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDictionary(t *testing.T) {
	t.Run("dictionary", func(t *testing.T) {
		d := dictionary("a", "b", "tag3", "tag4")
		assert.Equal(t, "a", d.Get(0))
		assert.Equal(t, "b", d.Get(1))
		assert.Equal(t, "tag4", d.Get(3))
		assert.Equal(t, "?", d.Get(10))
	})
}

func word(s string) DictWord {
	return DictWord{0, len(s), s}
}

func dictionary(words ...string) Dictionary {
	res := []DictWord{}
	for i, w := range words {
		res = append(res, DictWord{i, len(w), w})
	}
	return Dictionary{List: res}
}
