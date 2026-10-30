package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAsHex(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		a := []byte{10, 20, 36}
		assert.Equal(t, "0A:14:24", AsHex(a, 30))
		a = []byte{10, 20, 36, 16, 15}
		assert.Equal(t, "0A:14:24:10:0F", AsHex(a, 30))
		a = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		assert.Equal(t, "01:02:03:04:05:06:07:08:09:0A:..", AsHex(a, 9))
		assert.Equal(t, "01:02:03:04:05:06:07:08:09:0A:0B:..", AsHex(a, 10))
		assert.Equal(t, "01:02:03:04:05:06:07:08:09:0A:0B:0C", AsHex(a, 15))
	})
}

func TestToHex(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		a := [16]byte{10, 20, 36}
		assert.Equal(t, "0A:14:24:00:00:00:00:00:00:00:00:00:00:00:00:00", ToHex(a))
		a = [16]byte{10, 20, 36, 16, 15}
		assert.Equal(t, "0A:14:24:10:0F:00:00:00:00:00:00:00:00:00:00:00", ToHex(a))
		a = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		assert.Equal(t, "01:02:03:04:05:06:07:08:09:0A:0B:0C:00:00:00:00", ToHex(a))
	})
}
