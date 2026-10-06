package clock

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClock(t *testing.T) {

	// delta +- 10ms because of delay in CI
	t.Run("real", func(t *testing.T) {
		tt := Now()

		diff := time.Since(tt)
		assert.True(t, diff < 10*time.Millisecond)  // actual current time +- 10ms
		assert.True(t, diff > -10*time.Millisecond) //

		diff2 := Since(tt)
		assert.True(t, diff2-diff < 10*time.Millisecond) // almost same (+- 10ms)
	})

	t1 := time.Date(2023, 11, 23, 9, 58, 1, 0, time.UTC)
	t2 := time.Date(2023, 11, 23, 10, 0, 1, 0, time.UTC)

	t.Run("mock", func(t *testing.T) {
		As(t1, func() {
			tt := Now()
			assert.Equal(t, t1, tt)
			assert.Equal(t, time.Duration(0), Since(t1))     // same
			assert.True(t, time.Since(tt) > 30*24*time.Hour) // in the past
			assert.Equal(t, 2*time.Minute, Since(t2))        // mocked "current time" + 2m

		})

		As(t2, func() {
			tt := Now()
			assert.Equal(t, t2, tt)
			assert.Equal(t, time.Duration(0), Since(t2)) // same
			assert.Equal(t, -2*time.Minute, Since(t1))   // mocked "current time" - 2m

		})
	})
}
