package parquet

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProjectedSchemaTracksCallV2 pins the projection twin: CallV2Projected
// must be exactly CallV2 minus the two blob-sized columns. A field added to
// one struct but not the other would silently widen (or narrow) the list-path
// projection; the name match between the two structs IS the projection.
func TestProjectedSchemaTracksCallV2(t *testing.T) {
	full := leafColumnsOf(t, parquet.SchemaOf(new(CallV2)))
	projected := leafColumnsOf(t, parquet.SchemaOf(new(CallV2Projected)))

	want := map[string]string{}
	for path, typ := range full {
		if path == "trace_blob" || path == "big_params_json" {
			continue
		}
		want[path] = typ
	}
	assert.Equal(t, want, projected)

	require.Contains(t, full, "trace_blob")
	require.Contains(t, full, "big_params_json")
}

// leafColumnsOf renders a schema as leaf-path → type+repetition, the shape
// the column name-matching operates on.
func leafColumnsOf(t *testing.T, s *parquet.Schema) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, path := range s.Columns() {
		leaf, ok := s.Lookup(path...)
		require.True(t, ok, "leaf %v must resolve", path)
		typ := leaf.Node.Type().String()
		if leaf.Node.Optional() {
			typ += " OPTIONAL"
		}
		if leaf.Node.Repeated() {
			typ += " REPEATED"
		}
		out[strings.Join(path, ".")] = typ
	}
	return out
}
