package parquet

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProjectedSchemaTracksCallV2 pins the projection twin: CallV2Projected
// must be exactly CallV2 minus the blob-sized columns (the trace blob, the
// big-param values, and the inline dictionary/suspend of the self-contained
// row). A field added to one struct but not the other would silently widen
// (or narrow) the list-path projection; the name match between the two
// structs IS the projection.
func TestProjectedSchemaTracksCallV2(t *testing.T) {
	full := leafColumnsOf(t, parquet.SchemaOf(new(CallV2)))
	projected := leafColumnsOf(t, parquet.SchemaOf(new(CallV2Projected)))

	blobSized := map[string]bool{
		"trace_blob": true, "big_params_json": true,
		"dict_words_json": true, "suspend_json": true,
	}
	want := map[string]string{}
	for path, typ := range full {
		if blobSized[path] {
			continue
		}
		want[path] = typ
	}
	assert.Equal(t, want, projected)

	for path := range blobSized {
		require.Contains(t, full, path)
	}
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
