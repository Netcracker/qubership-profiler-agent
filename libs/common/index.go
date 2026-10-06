package common

import (
	"fmt"
	"strings"
)

// NormalizeParam removes all non-alphanumeric characters from the input string,
// converts it to lowercase, and ensures the result is suitable for use as a stable identifier.
// Returns an error if the result is empty or starts with a digit.
func NormalizeParam(param string) (string, error) {
	param = strings.ToLower(param)

	var sb strings.Builder
	for i := 0; i < len(param); i++ {
		c := param[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			sb.WriteByte(c)
		}
	}

	normalized := sb.String()

	if len(normalized) == 0 {
		return "", fmt.Errorf("parameter normalization failed: result is empty after stripping")
	}
	if normalized[0] >= '0' && normalized[0] <= '9' {
		return "", fmt.Errorf("parameter normalization failed: result starts with a digit (%q)", normalized)
	}

	return normalized, nil
}
