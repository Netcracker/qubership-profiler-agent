package common

import (
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
	"time"
)

func TestRandom(t *testing.T) {

	t.Run("valid", func(t *testing.T) {
		v := Random(10, 400)
		assert.GreaterOrEqual(t, v, int64(10))
		assert.LessOrEqual(t, v, int64(400))

		v = Random(10, 10)
		assert.Equal(t, int64(10), v)

		v = Random(20, 10)
		assert.GreaterOrEqual(t, v, int64(0))
		assert.LessOrEqual(t, v, int64(10))
	})
}

func TestRandomTime(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		tt := time.Now()
		t1 := tt.Truncate(time.Hour)
		t2 := t1.Add(time.Hour)
		rt := RandomTime(tt)
		assert.True(t, rt.Compare(t1) >= 0, "invalid random [%v] for %v [%v - %v]", rt, tt, t1, t2)
		assert.True(t, rt.Compare(t2) <= 0, "invalid random [%v] for %v [%v - %v]", rt, tt, t1, t2)
	})
	t.Run("utc", func(t *testing.T) {
		tt := time.Now()
		t1 := tt.UTC().Truncate(time.Hour)
		t2 := t1.UTC().Add(time.Hour)
		rt := RandomUtcTime(tt)
		assert.True(t, rt.Compare(t1) >= 0, "invalid random [%v] for %v [%v - %v]", rt, tt, t1, t2)
		assert.True(t, rt.Compare(t2) <= 0, "invalid random [%v] for %v [%v - %v]", rt, tt, t1, t2)
	})
}

func TestRandomUuid(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		s, err := RandomUuidVal()
		assert.Nil(t, err)
		assert.Equal(t, 16, len(s), "invalid uuid size: %s", s)
	})
	t.Run("string", func(t *testing.T) {
		s := RandomUuidString()
		assert.Equal(t, 47, len(s))
		res, err := regexp.MatchString("^([0-9A-F][0-9A-F]:)+[0-9A-F][0-9A-F]$", s)
		assert.Nil(t, err)
		assert.True(t, res, "invalid uuid format: %s", s)
	})
	t.Run("struct", func(t *testing.T) {
		s := RandomUuid()
		assert.Equal(t, 16, len(s.Val))
		assert.Equal(t, 47, len(s.Str))
	})
}
