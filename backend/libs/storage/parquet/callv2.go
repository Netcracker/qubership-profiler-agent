package parquet

// CallV2 is the seal-pass parquet row (01-write-contract.md §5.2). Every file
// is ZSTD-compressed and holds rows of exactly one retention class, sorted by
// (ts_ms DESC, pk ASC); the writer lives in libs/collector/hotstore.
//
// The struct tags use the parquet-go/parquet-go dialect. Column NAMES are the
// compatibility contract: the library matches a file's columns to this struct
// by name, so adding or removing a column stays backward-readable (a missing
// column reads as zero/NULL), while renaming a column or changing its type
// does not — those need a reader keyed on the SchemaVersion footer stamp.
//
// Column notes that the tags cannot carry:
//   - counters annotated UINT_* in the old CallParquet stay plain INT64 here:
//     the values are always non-negative, and parquet's unsigned converted
//     types are poorly supported in some readers (§5.3).
//   - trace_blob is NULL (not empty) when the blob was truncated; the reason
//     is in truncated_reason.
type CallV2 struct {
	// identity
	TsMs           int64  `parquet:"ts_ms"`
	PodId          string `parquet:"pod_id,dict"`
	RestartTimeMs  int64  `parquet:"restart_time_ms"`
	TraceFileIndex int32  `parquet:"trace_file_index"`
	BufferOffset   int32  `parquet:"buffer_offset"`
	RecordIndex    int32  `parquet:"record_index"`
	ThreadName     string `parquet:"thread_name,dict"`

	// dimensions
	Namespace   string `parquet:"namespace,dict"`
	ServiceName string `parquet:"service_name,dict"`
	PodName     string `parquet:"pod_name,dict"`
	Method      string `parquet:"method,dict"`

	// metrics (raw, not aggregated)
	DurationMs    int32 `parquet:"duration_ms"`
	CpuTimeMs     int64 `parquet:"cpu_time_ms"`
	WaitTimeMs    int64 `parquet:"wait_time_ms"`
	MemoryUsed    int64 `parquet:"memory_used"`
	QueueWaitMs   int32 `parquet:"queue_wait_ms"`
	SuspendMs     int32 `parquet:"suspend_ms"`
	ChildCalls    int32 `parquet:"child_calls"`
	Transactions  int32 `parquet:"transactions"`
	LogsGenerated int64 `parquet:"logs_generated"`
	LogsWritten   int64 `parquet:"logs_written"`
	FileRead      int64 `parquet:"file_read"`
	FileWritten   int64 `parquet:"file_written"`
	NetRead       int64 `parquet:"net_read"`
	NetWritten    int64 `parquet:"net_written"`

	// classification (re-derived at seal; 01-write-contract.md §5.6)
	ErrorFlag      bool   `parquet:"error_flag"`
	RetentionClass string `parquet:"retention_class,dict"`

	// semi-structured
	Params          Parameters `parquet:"params" parquet-value:",list"`
	TraceBlob       []byte     `parquet:"trace_blob,optional"`
	TruncatedReason *string    `parquet:"truncated_reason,optional,dict"`

	// big_params_json inlines the call's big-parameter values, resolved at
	// seal from the sql / xml value segments the blob references — the
	// segments themselves never reach S3, so this column is the cold tier's
	// only source for them (01-write-contract.md §4.4). A JSON object
	// {"<stream>:<seq>:<offset>": value}; NULL when the call has none. A
	// single scalar column (not a MAP) so the list-path projection can drop
	// it the same way it drops trace_blob.
	BigParamsJson *string `parquet:"big_params_json,optional"`

	// dict_words_json inlines the dictionary subset trace_blob references —
	// the method and param-key names, keyed by wire dictionary id: a JSON
	// object {"<id>": "<word>"}. The cold /tree path resolves the tree from
	// this column alone, so a sealed row is self-contained: there is no
	// separate dictionary snapshot whose TTL could dangle (01 §3.6, №3, №23).
	// NULL when the blob is NULL.
	DictWordsJson *string `parquet:"dict_words_json,optional"`

	// suspend_json inlines the stop-the-world pauses overlapping this call's
	// blob event-time span (the trace timer axis the tree renders node
	// windows on): a JSON array [{"end_ms": ..., "duration_ms": ...}] on the
	// shape of the internal suspend endpoint (each pause spans
	// [end_ms − duration_ms, end_ms]). The cold /tree path derives the
	// per-node suspension from it, so the row needs no suspend snapshot
	// either (01 §3.6, №3). NULL when the blob is NULL or no pause overlaps
	// the call.
	SuspendJson *string `parquet:"suspend_json,optional"`
}

// CallV2Projected is the list-path read shape (02-read-contract.md §5.4,
// §2.3.2): CallV2 minus the blob-sized columns. Reading through it makes the
// library mask the trace_blob and big_params_json chunks, so their pages are
// never fetched. Field tags must stay identical to their CallV2 twins — the
// name match IS the projection.
type CallV2Projected struct {
	TsMs           int64  `parquet:"ts_ms"`
	PodId          string `parquet:"pod_id,dict"`
	RestartTimeMs  int64  `parquet:"restart_time_ms"`
	TraceFileIndex int32  `parquet:"trace_file_index"`
	BufferOffset   int32  `parquet:"buffer_offset"`
	RecordIndex    int32  `parquet:"record_index"`
	ThreadName     string `parquet:"thread_name,dict"`

	Namespace   string `parquet:"namespace,dict"`
	ServiceName string `parquet:"service_name,dict"`
	PodName     string `parquet:"pod_name,dict"`
	Method      string `parquet:"method,dict"`

	DurationMs    int32 `parquet:"duration_ms"`
	CpuTimeMs     int64 `parquet:"cpu_time_ms"`
	WaitTimeMs    int64 `parquet:"wait_time_ms"`
	MemoryUsed    int64 `parquet:"memory_used"`
	QueueWaitMs   int32 `parquet:"queue_wait_ms"`
	SuspendMs     int32 `parquet:"suspend_ms"`
	ChildCalls    int32 `parquet:"child_calls"`
	Transactions  int32 `parquet:"transactions"`
	LogsGenerated int64 `parquet:"logs_generated"`
	LogsWritten   int64 `parquet:"logs_written"`
	FileRead      int64 `parquet:"file_read"`
	FileWritten   int64 `parquet:"file_written"`
	NetRead       int64 `parquet:"net_read"`
	NetWritten    int64 `parquet:"net_written"`

	ErrorFlag      bool   `parquet:"error_flag"`
	RetentionClass string `parquet:"retention_class,dict"`

	Params          Parameters `parquet:"params" parquet-value:",list"`
	TruncatedReason *string    `parquet:"truncated_reason,optional,dict"`
}

// SchemaVersionKey and SchemaVersion stamp the CallV2 shape into every sealed
// file's key-value footer metadata. Additive changes (and column removals) do
// NOT bump the version — the reader null-fills by column name. Bump it only
// on a non-additive change (type change, rename, semantic change), so a
// future reader can branch on the stamp before touching the rows.
//
// Version 3: self-contained rows (dict_words_json + suspend_json inline,
// №3/№23). The columns are additive, but the READ contract changed with
// them — a version-3 reader resolves trees from the row alone and never
// consults the dictionaries/v1 or suspend/v1 snapshots, which version-2
// writers required and version-3 deployments no longer write. Per the
// rollout decision (remediation 00-plan.md, decision 1) version-2 data is
// wiped, not migrated.
const (
	SchemaVersionKey = "profiler.schema_version"
	SchemaVersion    = "3"
)
