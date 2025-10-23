package queries

const (
	// Create queries

	InsertS3File = `INSERT INTO %s (
		uuid, start_time, end_time, file_type, dump_type, namespace, duration_range, file_name, status, services, 
		created_time, api_version, rows_count, file_size, remote_storage_path, local_file_path) 
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`

	InsertTempTableInventory = `INSERT INTO %s 
		(uuid, start_time, end_time, status, table_type, table_name, created_time, rows_count, table_size, table_total_size) 
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	// Read queries

	GetTempTableByStatusAndStartTimeBetween = `SELECT * FROM %s WHERE status = $1 AND start_time >= $2 AND start_time <= $3`

	GetInventoryByStartTimeBetween = `SELECT * FROM %s WHERE start_time >= $1 AND start_time <= $2`

	GetInventoryByEndTimeBetween = `SELECT * FROM %s WHERE end_time >= $1 AND end_time <= $2`

	GetTempTablesNames = `SELECT DISTINCT(table_name) FROM %s ORDER BY table_name ASC`

	CheckTempTableExists = `SELECT EXISTS ( SELECT 1 FROM temp_table_inventory WHERE table_name = $1)`

	GetS3FilesByDurationRangeAndStartTimeBetween = `SELECT * FROM %s WHERE file_type = $1 AND duration_range = $2 AND start_time >= $3 AND start_time <= $4`

	GetS3FilesByDumpTypeAndStartTimeBetween = `SELECT * FROM %s WHERE file_type = $1 AND dump_type = $2 AND start_time >= $3 AND start_time <= $4`

	GetS3FilesByTypeAndStartTimeBetween = `SELECT * FROM %s WHERE file_type = $1 AND start_time >= $2 AND start_time <= $3`

	// Update queries

	UpdateTempTableInventory = `UPDATE %s SET status=$1, rows_count=$2, table_size=$3, table_total_size=$4 WHERE uuid=$5`

	UpdateS3Files = `UPDATE %s SET
		status=$1, services=$2, rows_count=$3, file_size=$4, remote_storage_path=$5
		WHERE uuid=$6`

	// Delete queries

	RemoveByUUID = `DELETE FROM %s
		WHERE uuid=$1`
)
