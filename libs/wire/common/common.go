package common

import (
	"encoding/json"
)

func MapToJsonString[objType any](m objType) string {
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}

// Ref is used to convert value to a pointer-to-value, e.g. for string or number literal
func Ref[T any](v T) *T {
	return &v
}
