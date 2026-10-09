package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToUuid(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		a := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		res := ToUuid(a)
		assert.Equalf(t, a, res.Val, "not equals (%v)", res.Val)
		assert.Equalf(t, a, res.ToBin(), "not equals (%v)", res.Val)
		assert.Equalf(t, "01:02:03:04:05:06:07:08:09:0A:0B:0C:00:00:00:00", res.Str, "invalid: %v", res)
		assert.Equalf(t, "01:02:03:04:05:06:07:08:09:0A:0B:0C:00:00:00:00", res.String(), "invalid: %v", res)
		assert.Equalf(t, "01:02:03:04:05:06:07:08:09:0A:0B:0C:00:00:00:00", res.ToHex(), "invalid: %v", res)
	})
}
