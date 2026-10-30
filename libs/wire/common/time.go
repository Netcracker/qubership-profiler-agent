package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func ParseDate(date string) (time.Time, error) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		return time.Now(), fmt.Errorf("error during loading of time-zone location: %s", err.Error())
	}

	return time.ParseInLocation("2006/01/02", date, location)
}

func DateHour(t time.Time) string {
	return t.UTC().Format("2006/01/02/15")
}

func ParseHourTime(date string) (time.Time, error) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		return time.Now(), fmt.Errorf("error during loading of time-zone location: %s", err.Error())
	}

	return time.ParseInLocation("2006/01/02/15", date, location)
}

// ParseGranularity parses a duration string that represents the time granularity
// used for temporary tables or other time-based data grouping.
// Only "m" (minutes) and "h" (hours) suffixes are allowed.
// Examples of valid input: "5m", "30m", "1h", "2h"
// Invalid input (e.g., "10s", "1d", "1.5h") will return an error.
func ParseGranularity(input string) (time.Duration, error) {
	// Validate that the unit is either 'm' or 'h'
	if strings.HasSuffix(input, "m") || strings.HasSuffix(input, "h") {
		return time.ParseDuration(input)
	}
	return 0, errors.New("invalid duration: only 'm' and 'h' suffixes are allowed")
}

// ParseLifetime parses a duration string that represents the retention period
// or "lifetime" of temporary data (e.g. heap dumps, temp tables).
// Only "h" (hours) and "d" (days) suffixes are allowed.
// - "h" is parsed using time.ParseDuration (e.g., "72h")
// - "d" is manually interpreted as 24 hours per day (e.g., "7d" = 168h)
// Floating-point day values like "1.5d" are not supported.
//
// This function is designed to restrict user input in config files (e.g., values.yaml)
// to safe, predictable duration units, avoiding unsupported or ambiguous formats.
func ParseLifetime(input string) (time.Duration, error) {
	if strings.HasSuffix(input, "h") {
		return time.ParseDuration(input)
	}
	if strings.HasSuffix(input, "d") {
		numStr := strings.TrimSuffix(input, "d")
		days, err := strconv.Atoi(numStr)
		if err != nil {
			return 0, errors.New("invalid day value in lifetime")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return 0, errors.New("invalid duration: only 'h' and 'd' suffixes are allowed")
}
