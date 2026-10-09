package common

import "testing"

func TestNormalizeParam(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		expectError bool
	}{
		// Valid cases
		{"X-B3-TraceId", "xb3traceid", false},
		{"job.jms.connection", "jobjmsconnection", false},
		{"FOO.BAR-abc", "foobarabc", false},
		{"___only__underscores__", "onlyunderscores", false},
		{"param-123", "param123", false},

		// Invalid: empty after cleanup
		{"", "", true},
		{"---___...", "", true},
		{"!@#$%^&*", "", true},

		// Invalid: starts with digit
		{"123-param", "", true},
		{"9.trace", "", true},
		{"0", "", true},

		// Edge: valid mix, ends with digits
		{"trace.id.123", "traceid123", false},
		{"param.1", "param1", false},
	}

	for _, tt := range tests {
		result, err := NormalizeParam(tt.input)
		if tt.expectError {
			if err == nil {
				t.Errorf("NormalizeParam(%q) expected error, got result: %q", tt.input, result)
			}
		} else {
			if err != nil {
				t.Errorf("NormalizeParam(%q) unexpected error: %v", tt.input, err)
			} else if result != tt.expected {
				t.Errorf("NormalizeParam(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		}
	}
}
