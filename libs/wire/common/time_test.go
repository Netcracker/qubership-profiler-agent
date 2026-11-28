package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		actual, err := ParseDate("2024/11/23")
		assert.Nil(t, err)
		tt := time.Date(2024, 11, 23, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, tt, actual)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseDate("2024/24/23")
		assert.ErrorContains(t, err, "month out of range")

		_, err = ParseDate("-12321/24/23")
		assert.ErrorContains(t, err, "cannot parse")

		_, err = ParseDate("")
		assert.ErrorContains(t, err, "cannot parse")
	})
}

func TestDateHour(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tt := time.Date(2024, 4, 3, 20, 30, 40, 0, time.UTC)
		actual := DateHour(tt)
		assert.Equal(t, "2024/04/03/20", actual)

		tt = time.Date(2024, 11, 31, 23, 54, 40, 0, time.UTC)
		actual = DateHour(tt)
		assert.Equal(t, "2024/12/01/23", actual)

		tt = time.Date(2024, 11, 31, 23, 05, 40, 0, time.UTC)
		assert.Equal(t, "2024-12-01T23:05:40Z", tt.Format(time.RFC3339))
		actual = DateHour(tt)
		assert.Equal(t, "2024/12/01/23", actual)

		loc := time.FixedZone("UTC-8", -8*60*60)
		tt = time.Date(2024, 11, 31, 23, 05, 40, 0, loc)
		assert.Equal(t, "2024-12-01T23:05:40-08:00", tt.Format(time.RFC3339))
		assert.Equal(t, "2024-12-02T07:05:40Z", tt.UTC().Format(time.RFC3339))
		actual = DateHour(tt)
		assert.Equal(t, "2024/12/02/07", actual)
	})
}

func TestParseHourTime(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		actual, err := ParseHourTime("2024/12/02/07")
		assert.Nil(t, err)
		assert.Equal(t, "2024-12-02T07:00:00Z", actual.Format(time.RFC3339))
		assert.Equal(t, "2024-12-02T07:00:00Z", actual.UTC().Format(time.RFC3339))
	})
	t.Run("invalid", func(t *testing.T) {
		_, err := ParseHourTime("asdasdas")
		assert.ErrorContains(t, err, "cannot parse")

		_, err = ParseHourTime("2024/24/24/242/")
		assert.ErrorContains(t, err, "month out of range")
	})
}

func TestParseGranularity(t *testing.T) {
	tests := []struct {
		input       string
		want        time.Duration
		expectError bool
	}{
		{"30m", 30 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"1.5h", time.Hour + 30*time.Minute, false},
		{"45s", 0, true},
		{"1d", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		got, err := ParseGranularity(tt.input)
		if tt.expectError {
			if err == nil {
				t.Errorf("expected error for input %q, got none", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for input %q: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("input %q: expected %v, got %v", tt.input, tt.want, got)
			}
		}
	}
}

func TestParseLifetime(t *testing.T) {
	tests := []struct {
		input       string
		want        time.Duration
		expectError bool
	}{
		{"72h", 72 * time.Hour, false},    // valid hours
		{"7d", 7 * 24 * time.Hour, false}, // valid days
		{"0d", 0, false},                  // zero days
		{"1d", 24 * time.Hour, false},     // single day
		{"1.5d", 0, true},                 // float days not supported
		{"30m", 0, true},                  // invalid unit (minutes not allowed)
		{"", 0, true},                     // empty string
		{"10x", 0, true},                  // unknown suffix
		{"abc", 0, true},                  // malformed input
	}

	for _, tt := range tests {
		got, err := ParseLifetime(tt.input)
		if tt.expectError {
			if err == nil {
				t.Errorf("expected error for input %q, got none", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for input %q: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("input %q: expected %v, got %v", tt.input, tt.want, got)
			}
		}
	}
}
