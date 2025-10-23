package db

type DBParams struct {
	DBHost        string
	DBPort        int
	DBUser        string
	DBPassword    string
	DBName        string
	EnableMetrics bool
}
