package envconfig

import (
	"path/filepath"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Log level
	LogLevel string `envconfig:"DIAG_LOG_LEVEL" default:"info"`
	// PV params
	PVMountPath string `envconfig:"DIAG_PV_MOUNT_PATH"`

	// DB params
	DBPath           string `envconfig:"DIAG_DB_PATH" default:""`          // SQLite database file path (e.g., /data/profiler_dumps.db or :memory:)
	DBName           string `envconfig:"DIAG_DB_NAME" default:"profiler_dumps.db"`
	DBMetricsEnabled bool   `envconfig:"DIAG_DB_METRICS_ENABLED" default:"false"`

	// Server params
	BindAddress string `envconfig:"DIAG_BIND_ADDRESS" default:":8000"`

	// Tasks params
	InsertCron   string `envconfig:"DIAG_PV_INSERT_CRON" default:"* * * * *"`   // Cron schedule for insert/index task
	ArchiveHours int    `envconfig:"DIAG_PV_HOURS_ARCHIVE_AFTER" default:"2"`
	ArchiveCron  string `envconfig:"DIAG_PV_ARCHIVE_CRON" default:"6 * * * *"`  // Cron schedule for archive/pack task
	DeleteDays   int    `envconfig:"DIAG_PV_DAYS_DELETE_AFTER" default:"14"`
	DeleteCron   string `envconfig:"DIAG_PV_DELETE_CRON" default:"30 * * * *"`  // Cron schedule for cleanup task
	MaxHeapDumps int    `envconfig:"DIAG_PV_MAX_HEAP_DUMPS_PER_POD" default:"10"`
}

func (c *Config) GetPathToDB() string {
	if c.DBPath != "" {
		return c.DBPath
	}
	return filepath.Join(c.PVMountPath, c.DBName)
}

func (c *Config) GetBasePVDir() string {
	return filepath.Join(c.PVMountPath, "diagnostic")
}

var EnvConfig Config

func InitConfig() error {
	return envconfig.Process("DIAG", &EnvConfig)
}
