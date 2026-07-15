package parquet

import (
	parquetgo "github.com/parquet-go/parquet-go"
)

// CallV2WriterOptions returns the writer options every CallV2 producer must
// share (01-write-contract.md §5.2): ZSTD on every file, the schema-version
// stamp in the key-value footer metadata, and no page bounds for the
// blob-sized columns — their min/max statistics would copy blob prefixes
// into the footer for columns nobody range-prunes on.
//
// The seal pass in libs/collector/hotstore still carries the same list
// inline; keep the two in sync until the collector adopts this helper.
func CallV2WriterOptions() []parquetgo.WriterOption {
	return []parquetgo.WriterOption{
		parquetgo.Compression(&parquetgo.Zstd),
		parquetgo.KeyValueMetadata(SchemaVersionKey, SchemaVersion),
		parquetgo.SkipPageBounds("trace_blob"),
		parquetgo.SkipPageBounds("big_params_json"),
	}
}
