package db

type DBParams struct {
	DBType        string // "postgres" or "sqlite"
	DBPath        string // SQLite database file path (empty for postgres)
	DBHost        string
	DBPort        int
	DBUser        string
	DBPassword    string
	DBName        string
	EnableMetrics bool
}
