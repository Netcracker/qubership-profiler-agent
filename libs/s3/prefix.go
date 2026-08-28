package s3

import "strings"

// KeyPrefix applies S3_PATH_PREFIX (01-write-contract.md §7, 04 §6) at the
// object-store boundary: the code on both sides of S3 keeps working with
// bucket-root-relative keys (parquet/v1/..., pods/v1/...), and only the
// store adapters translate to and from the deployment's prefixed key space.
// Applying it in ONE place keeps every producer and consumer — seal upload,
// pods manifests, discovery LISTs, maintenance — on the same prefix by
// construction.
type KeyPrefix string

// NewKeyPrefix normalizes the raw S3_PATH_PREFIX value: surrounding slashes
// and whitespace are dropped, so "", "/", "team-a/" and "team-a" behave
// predictably. Interior slashes are kept — a multi-segment prefix is valid.
func NewKeyPrefix(raw string) KeyPrefix {
	return KeyPrefix(strings.Trim(strings.TrimSpace(raw), "/"))
}

// Apply roots a bucket-root-relative key under the prefix.
func (p KeyPrefix) Apply(key string) string {
	if p == "" {
		return key
	}
	return string(p) + "/" + key
}

// Strip translates a listed object key back into the bucket-root-relative
// space. ok is false for a key outside the prefix — a LIST under an applied
// prefix cannot return one, so callers may skip such keys.
func (p KeyPrefix) Strip(key string) (string, bool) {
	if p == "" {
		return key, true
	}
	return strings.CutPrefix(key, string(p)+"/")
}
