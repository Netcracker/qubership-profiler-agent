package utils

// Ref is used to convert value to a pointer-to-value, e.g. for string or number literal
func Ref[T any](v T) *T {
	return &v
}
