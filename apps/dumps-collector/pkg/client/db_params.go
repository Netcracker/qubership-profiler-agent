package db

import "time"

const Granularity = time.Hour

type DBParams struct {
	DBPath        string // SQLite database file path (e.g., /data/profiler_dumps.db or :memory:)
	EnableMetrics bool
}
