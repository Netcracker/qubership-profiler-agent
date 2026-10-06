package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKeyPrefix pins the S3_PATH_PREFIX translation the store adapters share:
// keys stay bucket-root-relative inside the code, the prefix applies only at
// the S3 boundary, and a LIST result strips back symmetrically.
func TestKeyPrefix(t *testing.T) {
	empty := NewKeyPrefix("")
	assert.Equal(t, "parquet/v1/x", empty.Apply("parquet/v1/x"))
	key, ok := empty.Strip("parquet/v1/x")
	assert.True(t, ok)
	assert.Equal(t, "parquet/v1/x", key)

	for _, raw := range []string{"team-a", "/team-a", "team-a/", " team-a/ ", "/team-a/"} {
		p := NewKeyPrefix(raw)
		assert.Equal(t, "team-a/parquet/v1/x", p.Apply("parquet/v1/x"), "raw %q", raw)
	}

	nested := NewKeyPrefix("org/team-a")
	assert.Equal(t, "org/team-a/pods/v1/x.json", nested.Apply("pods/v1/x.json"))
	key, ok = nested.Strip("org/team-a/pods/v1/x.json")
	assert.True(t, ok)
	assert.Equal(t, "pods/v1/x.json", key)

	_, ok = nested.Strip("other/pods/v1/x.json")
	assert.False(t, ok, "a foreign key does not strip")
}
