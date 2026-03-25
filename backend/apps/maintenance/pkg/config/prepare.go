package config

import (
	"context"
	"fmt"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"os"
	"strings"
	"time"
)

func prepareConfig(ctx context.Context) (*Config, error) {
	cronSchedule := prepareCronSchedule(ctx, os.Getenv("CRON_SCHEDULE"))

	jobConfig, err := prepareJobConfig(ctx)
	if err != nil {
		return nil, err
	}

	s3Params, err := prepareS3Config(ctx)
	if err != nil {
		return nil, err
	}
	pgParams, err := preparePostgresConfig(ctx)
	if err != nil {
		return nil, err
	}

	invertedIndexConfig, err := prepareInvertedIndexConfig(ctx)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		CronRun:             cronRun,
		CronJobSchedule:     cronSchedule,
		JobConfig:           jobConfig,
		S3:                  s3Params,
		Postgres:            pgParams,
		InvertedIndexConfig: invertedIndexConfig,
	}
	return cfg, nil
}

func prepareJobConfig(ctx context.Context) (*JobConfig, error) {
	return ParseConfigFromFile(ctx, jobConfigLocation)
}

func prepareCronSchedule(ctx context.Context, cronSchedule string) string {
	if cronSchedule == "" {
		log.Info(ctx, "Env variable CRON_SCHEDULE is empty. Default cron schedule is '%s'", DefaultCronSchedule)
		cronSchedule = DefaultCronSchedule
	}
	return cronSchedule
}

func prepareS3Config(_ context.Context) (*s3.Params, error) {

	s3params := &s3.Params{
		Endpoint:        minioEndpoint,
		AccessKeyID:     minioAccessKey,
		SecretAccessKey: minioSecretKey,
		InsecureSSL:     minioInsecureSSL,
		UseSSL:          minioUseSSL,
		BucketName:      minioBucket,
		CAFile:          minioPathToCA,
	}

	if s3params.IsEmpty() {
		return nil, fmt.Errorf("no credentials for S3. " +
			"In order to upload data to S3 provide minio.url/minio.key/minio.secret cli arguments" +
			" or env variables MINIO_ENDPOINT/MINIO_ACCESS_KEY_ID/MINIO_SECRET_ACCESS_KEY")
	}

	s3params.Prepare()

	if err := s3params.IsValid(); err != nil {
		return nil, err
	}

	return s3params, nil
}

func preparePostgresConfig(_ context.Context) (*pg.Params, error) {
	connStr := pgConnectionUrl

	if connStr == "" {
		url := os.Getenv("POSTGRES_URL")
		user := os.Getenv("POSTGRES_USER")
		password := os.Getenv("POSTGRES_PASSWORD")
		dbName := os.Getenv("POSTGRES_DB")

		if url == "" || user == "" || password == "" || dbName == "" {
			return nil, fmt.Errorf("no credentials for Postgres. " +
				"In order to work with Postgres DB provide pg.url cli arguments or env variables POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_URL")
		}

		connStr = fmt.Sprintf("postgres://%s:%s@%s/%s", user, password, url, dbName)
	}

	pgParams := &pg.Params{
		ConnStr: connStr,
		SSLMode: pgSslMode,
		CAFile:  pgPathToCA,
	}

	if err := pgParams.IsValid(); err != nil {
		return nil, err
	}

	return pgParams, nil
}

func prepareInvertedIndexConfig(ctx context.Context) (*index.InvertedIndexConfig, error) {

	granularityStr := os.Getenv("INVERTED_INDEX_GRANULARITY")
	var granularity time.Duration
	if granularityStr == "" {
		granularity = pg.InvertedIndexGranularity
		log.Info(ctx, "Env variable INVERTED_INDEX_GRANULARITY is empty. Default value will be used: %s", granularity)
	} else {
		var err error
		granularity, err = common.ParseGranularity(granularityStr)
		if err != nil {
			return nil, fmt.Errorf("invalid granularity duration format: %w", err)
		}
	}

	lifetimeStr := os.Getenv("INVERTED_INDEX_LIFETIME")
	var lifetime time.Duration
	if lifetimeStr == "" {
		lifetime = pg.InvertedIndexLifetime
		log.Info(ctx, "Env variable INVERTED_INDEX_LIFETIME is empty. Default value will be used: %s", lifetime)
	} else {
		var err error
		lifetime, err = common.ParseLifetime(lifetimeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid lifetime duration format: %w", err)
		}
	}

	paramsRaw := os.Getenv("INVERTED_INDEX_PARAMS")
	if paramsRaw == "" {
		paramsRaw = pg.InvertedIndexParams
		log.Info(ctx, "Env variable INVERTED_INDEX_PARAMS is empty. Default value is '%s'", paramsRaw)
	}
	params := strings.Split(paramsRaw, ",")

	prefixes := make([]string, 0, len(params))
	for _, p := range params {
		prefix, err := common.NormalizeParam(p)
		if err != nil {
			return nil, fmt.Errorf("invalid param format: %w", err)
		}
		prefixes = append(prefixes, prefix)
	}

	config := &index.InvertedIndexConfig{
		Granularity: granularity,
		Lifetime:    lifetime,
		Params:      params,
		Prefixes:    prefixes,
	}

	return config, nil
}
