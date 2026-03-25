package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDurationAsInt(t *testing.T) {
	t.Run("convert", func(t *testing.T) {
		for _, d := range Durations.List {
			assert.Equal(t, d.From, DurationAsInt(&d), "invalid convert for %v", d)
		}
	})
}

func TestDurationRanges_Get(t *testing.T) {
	t.Run("convert", func(t *testing.T) {
		assert.Equal(t, Durations.List[0], Durations.Get(0))
		assert.Equal(t, Durations.List[1], Durations.Get(1))
		assert.Equal(t, Durations.List[2], Durations.Get(10)) // 10ms
		assert.Equal(t, Durations.List[2], Durations.Get(11))
		assert.Equal(t, Durations.List[3], Durations.Get(123))
		assert.Equal(t, Durations.List[4], Durations.Get(1000))  // 1s
		assert.Equal(t, Durations.List[6], Durations.Get(89000)) // -> 30s
		assert.Equal(t, Durations.List[7], Durations.Get(90000)) // 90s
		assert.Equal(t, Durations.List[7], Durations.Get(91000))
		assert.Equal(t, Durations.List[7], Durations.Get(100000))
	})
}

func TestDurationRanges_GetByName(t *testing.T) {
	t.Run("parse", func(t *testing.T) {
		assert.Equal(t, Durations.List[0], *Durations.GetByName("0ms"))
		assert.Equal(t, Durations.List[1], *Durations.GetByName("1ms"))
		assert.Equal(t, Durations.List[2], *Durations.GetByName("10ms"))
		assert.Equal(t, Durations.List[3], *Durations.GetByName("100ms"))
		assert.Equal(t, Durations.List[4], *Durations.GetByName("1s"))
		assert.Equal(t, Durations.List[5], *Durations.GetByName("5s"))
		assert.Equal(t, Durations.List[6], *Durations.GetByName("30s"))
		assert.Equal(t, Durations.List[7], *Durations.GetByName("90s"))
	})
	t.Run("invalid", func(t *testing.T) {
		assert.Nil(t, Durations.GetByName(""))
		assert.Nil(t, Durations.GetByName("ASDADASD"))
		assert.Nil(t, Durations.GetByName("0s"))
		assert.Nil(t, Durations.GetByName("180s"))
		assert.Nil(t, Durations.GetByName("10m"))

	})
}
