package parquet

// CallV2 is the seal-pass parquet row (01-write-contract.md §5.2). Every file
// is ZSTD-compressed and holds rows of exactly one retention class, sorted by
// (ts_ms DESC, pk ASC); the writer lives in libs/collector/hotstore.
//
// Column notes that the tags cannot carry:
//   - counters annotated UINT_* in the old CallParquet stay plain INT64 here:
//     the values are always non-negative, and parquet's unsigned converted
//     types are poorly supported in some readers (§5.3).
//   - trace_blob is NULL (not empty) when the blob was truncated; the reason
//     is in truncated_reason.
type CallV2 struct {
	// identity
	TsMs           int64  `parquet:"name=ts_ms, type=INT64"`
	PodId          string `parquet:"name=pod_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RestartTimeMs  int64  `parquet:"name=restart_time_ms, type=INT64"`
	TraceFileIndex int32  `parquet:"name=trace_file_index, type=INT32"`
	BufferOffset   int32  `parquet:"name=buffer_offset, type=INT32"`
	RecordIndex    int32  `parquet:"name=record_index, type=INT32"`
	ThreadName     string `parquet:"name=thread_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	// dimensions
	Namespace   string `parquet:"name=namespace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ServiceName string `parquet:"name=service_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PodName     string `parquet:"name=pod_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Method      string `parquet:"name=method, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	// metrics (raw, not aggregated)
	DurationMs    int32 `parquet:"name=duration_ms, type=INT32"`
	CpuTimeMs     int64 `parquet:"name=cpu_time_ms, type=INT64"`
	WaitTimeMs    int64 `parquet:"name=wait_time_ms, type=INT64"`
	MemoryUsed    int64 `parquet:"name=memory_used, type=INT64"`
	QueueWaitMs   int32 `parquet:"name=queue_wait_ms, type=INT32"`
	SuspendMs     int32 `parquet:"name=suspend_ms, type=INT32"`
	ChildCalls    int32 `parquet:"name=child_calls, type=INT32"`
	Transactions  int32 `parquet:"name=transactions, type=INT32"`
	LogsGenerated int64 `parquet:"name=logs_generated, type=INT64"`
	LogsWritten   int64 `parquet:"name=logs_written, type=INT64"`
	FileRead      int64 `parquet:"name=file_read, type=INT64"`
	FileWritten   int64 `parquet:"name=file_written, type=INT64"`
	NetRead       int64 `parquet:"name=net_read, type=INT64"`
	NetWritten    int64 `parquet:"name=net_written, type=INT64"`

	// classification (re-derived at seal; 01-write-contract.md §5.6)
	ErrorFlag      bool   `parquet:"name=error_flag, type=BOOLEAN"`
	RetentionClass string `parquet:"name=retention_class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	// semi-structured
	Params          Parameters `parquet:"name=params, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
	TraceBlob       *string    `parquet:"name=trace_blob, type=BYTE_ARRAY, repetitiontype=OPTIONAL"`
	TruncatedReason *string    `parquet:"name=truncated_reason, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`

	// big_params_json inlines the call's big-parameter values, resolved at
	// seal from the sql / xml value segments the blob references — the
	// segments themselves never reach S3, so this column is the cold tier's
	// only source for them (01-write-contract.md §4.4). A JSON object
	// {"<stream>:<seq>:<offset>": value}; NULL when the call has none. A
	// single scalar column (not a MAP) so the list-path projection can drop
	// it the same way it drops trace_blob.
	BigParamsJson *string `parquet:"name=big_params_json, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}
