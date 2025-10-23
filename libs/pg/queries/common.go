package queries

const (
	// Read queries

	GetTableSize = `SELECT pg_relation_size('%s')`

	GetTotalTableSize = `SELECT pg_total_relation_size('%s')`

	GetRowsCount = `SELECT COUNT(*) FROM %s`

	// Delete queries

	DropTables = `DROP TABLE IF EXISTS %s CASCADE`

	TruncateTable = `TRUNCATE TABLE %s`
)
