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
	DBHost           string `envconfig:"DIAG_POSTGRES_HOST" default:"localhost"`
	DBPort           int    `envconfig:"DIAG_POSTGRES_PORT" default:"5432"`
	DBUser           string `envconfig:"DIAG_POSTGRES_USERNAME" default:"postgres"`
	DBPassword       string `envconfig:"DIAG_POSTGRES_PASSWORD" default:"postgres"`
	DBName           string `envconfig:"DIAG_DB_NAME" default:"profiler_dumps"`
	DBMetricsEnabled bool   `envconfig:"DIAG_DB_METRICS_ENABLED" default:"false"`

	// Server params
	BindAddress string `envconfig:"DIAG_BIND_ADDRESS" default:":8000"`

	// Tasks params
	ArchiveHours int `envconfig:"DIAG_PV_HOURS_ARCHIVE_AFTER" default:"2"`
	DeleteDays   int `envconfig:"DIAG_PV_DAYS_DELETE_AFTER" default:"14"`
	MaxHeapDumps int `envconfig:"DIAG_PV_MAX_HEAP_DUMPS_PER_POD" default:"10"`
}

func (c *Config) GetPathToDB() string {
	return filepath.Join(c.PVMountPath, c.DBName)
}

func (c *Config) GetBasePVDir() string {
	return filepath.Join(c.PVMountPath, "diagnostic")
}

var EnvConfig Config

func InitConfig() error {
	return envconfig.Process("DIAG", &EnvConfig)
}
