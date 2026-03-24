package queries

const (
	// Create queries

	InsertCall = `INSERT INTO %s (
		time, cpu_time, wait_time, memory_used, duration, non_blocking, queue_wait_duration, suspend_duration, 
		calls, transactions, logs_generated, logs_written, file_read, file_written, net_read, net_written, 
		namespace, service_name, pod_name, restart_time, 
		method, params, trace_file_index, buffer_offset, record_index) 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)`

	InsertTrace = `INSERT INTO %s (
		pod_name, restart_time, 
		trace_file_index, buffer_offset, record_index, 
		trace) 
		VALUES ($1, $2, $3, $4, $5, $6)`

	InsertDump = `INSERT INTO %s (
		uuid, created_time,
		namespace, service_name, pod_name, restart_time, pod_type,
		dump_type, bytes_size, info, binary_data) 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	InsertInvertedIndex = `INSERT INTO %s (value, file_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`

	// Read queries

	GetCallsTimeBetween = `SELECT c.*
		FROM %s c
		WHERE c.namespace = $1 AND c.time < $2 AND c.time >= $3
		ORDER BY service_name ASC, pod_name ASC, time DESC`

	GetCallsWithTraceTimeBetween = `SELECT c.*, t.trace
		FROM %s c
		LEFT JOIN %s t
		ON c.pod_name = t.pod_name AND c.restart_time = t.restart_time AND c.trace_file_index = t.trace_file_index AND c.buffer_offset = t.buffer_offset AND c.record_index = t.record_index
		WHERE c.namespace = $1 AND c.service_name = $2 AND c.pod_name = $3 AND c.time < $4 AND c.time >= $5
		ORDER BY service_name ASC, pod_name ASC, time DESC`

	GetDumpsTimeBetween = `SELECT * FROM %s WHERE namespace = $1 AND service_name = $2 AND pod_name = $3 AND created_time < $4 AND created_time >= $5
		ORDER BY service_name ASC, pod_name ASC, created_time DESC`

	// Update queries

	// Delete queries

)
