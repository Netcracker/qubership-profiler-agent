package config

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
)

var Cfg *Config

type (
	Config struct {
		MigrateOnly         bool
		CronRun             bool
		CronJobSchedule     string
		JobConfig           *JobConfig
		S3                  *s3.Params
		Postgres            *pg.Params
		InvertedIndexConfig *index.InvertedIndexConfig
	}
)

func PrepareConfig(ctx context.Context) error {
	var err error
	Cfg, err = prepareConfig(ctx)
	return err
}

func PreparePGConfig(ctx context.Context) (*pg.Params, error) {
	return preparePostgresConfig(ctx)
}
