package data

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestPrepareTimeRanges(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("invalid start date", func(t *testing.T) {
		args := []string{"--startdate=1234/56/78/90"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "invalid date format for startdate, must be in the format yyyy/mm/dd")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("invalid end date", func(t *testing.T) {
		args := []string{"--enddate=1234/56/78/90"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "invalid date format for enddate, must be in the format yyyy/mm/dd")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("start date after end date", func(t *testing.T) {
		args := []string{"--startdate=2024/06/02", "--enddate=2024/06/01"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "start date must be before the end date")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("invalid start time", func(t *testing.T) {
		args := []string{"--starttime=/1234/56/78/90/"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "invalid time format for starttime, must be in the format yyyy/mm/dd/hh")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("skip time range arguments if only parse", func(t *testing.T) {
		args := []string{"--parse", "--startdate=1234/56/78/90", "--enddate=1234/56/78/90", "--hours=-1"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, timeRange)
			assert.Equal(t, time.UnixMilli(0), timeRange.StartDate)
			assert.Equal(t, time.UnixMilli(0), timeRange.EndDate)
			assert.Equal(t, time.UnixMilli(0), timeRange.HourDateTime)
			assert.Equal(t, 1, timeRange.HoursCount)
		}, nil, args...)
	})

	t.Run("too big time range", func(t *testing.T) {
		args := []string{"--startdate=2024/05/01", "--enddate=2024/06/01"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "too long time period (745 hours), must be not greater than 31*24 hours")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("invalid hours", func(t *testing.T) {
		args := []string{"--hours=0"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "invalid hours count (0 hours), must be between 0 and 4 hours")
			assert.Nil(t, timeRange)
		}, nil, args...)

		args = []string{"--hours=24"}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.ErrorContains(t, err, "invalid hours count (24 hours), must be between 0 and 4 hours")
			assert.Nil(t, timeRange)
		}, nil, args...)
	})

	t.Run("valid defaults", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			timeRange, err := prepareTimeRange(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, timeRange)
		}, nil, args...)
	})
}

func TestPrepareLimit(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("wrong number of ns/svc/pods", func(t *testing.T) {
		args := []string{"--namespaces=-1"}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.ErrorContains(t, err, "the number of namespaces, services and pods must be greater than zero")
			assert.Nil(t, limits)
		}, nil, args...)

		args = []string{"--services=-100"}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.ErrorContains(t, err, "the number of namespaces, services and pods must be greater than zero")
			assert.Nil(t, limits)
		}, nil, args...)

		args = []string{"--pods=0"}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.ErrorContains(t, err, "the number of namespaces, services and pods must be greater than zero")
			assert.Nil(t, limits)
		}, nil, args...)
	})

	t.Run("default calls", func(t *testing.T) {
		args := []string{}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, limits)
			assert.Equal(t, 100, limits.Calls)
		}, nil, args...)
	})

	t.Run("override calls", func(t *testing.T) {
		args := []string{"--calls=5"}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, limits)
			assert.Equal(t, 5, limits.Calls)
		}, nil, args...)
	})

	t.Run("valid defaults", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			limits, err := prepareLimit(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, limits)
		}, nil, args...)
	})
}

func TestS3Params(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("no default credentials", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			s3Params, err := prepareS3Params(ctx)
			assert.ErrorContains(t, err, "no credentials for S3")
			assert.Nil(t, s3Params)
		}, nil, args...)
	})

	t.Run("values from env", func(t *testing.T) {
		args := []string{""}
		envs := map[string]string{
			"MINIO_ENDPOINT":          "localhost:9000",
			"MINIO_ACCESS_KEY_ID":     "test",
			"MINIO_SECRET_ACCESS_KEY": "test12345",
			"MINIO_BUCKET":            "test_bucket",
		}
		mockArgs(func() {
			s3Params, err := prepareS3Params(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, s3Params)
			assert.Equal(t, "localhost:9000", s3Params.Endpoint)
			assert.Equal(t, "test", s3Params.AccessKeyID)
			assert.Equal(t, "test12345", s3Params.SecretAccessKey)
			assert.Equal(t, "test_bucket", s3Params.BucketName)
		}, envs, args...)
	})

	t.Run("values from args", func(t *testing.T) {
		args := []string{"--minio.url=localhost:9000", "--minio.key=test", "--minio.secret=test12345", "--minio.insecure", "--minio.use_ssl"}
		mockArgs(func() {
			s3Params, err := prepareS3Params(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, s3Params)
			assert.Equal(t, "localhost:9000", s3Params.Endpoint)
			assert.Equal(t, "test", s3Params.AccessKeyID)
			assert.Equal(t, "test12345", s3Params.SecretAccessKey)
			assert.Equal(t, "profiler", s3Params.BucketName)
			assert.Equal(t, true, s3Params.InsecureSSL)
			assert.Equal(t, true, s3Params.UseSSL)
		}, nil, args...)
	})
}

func TestPGParams(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("no default credentials", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			pgParams, err := preparePGParams(ctx)
			assert.ErrorContains(t, err, "no credentials for Postgres")
			assert.Nil(t, pgParams)
		}, nil, args...)
	})

	t.Run("values from env", func(t *testing.T) {
		args := []string{""}
		envs := map[string]string{
			"POSTGRES_URL":      "localhost:5432",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "cdt_test",
		}
		mockArgs(func() {
			pgParams, err := preparePGParams(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, pgParams)
			assert.Equal(t, "postgres://postgres:postgres@localhost:5432/cdt_test", pgParams.ConnStr)
		}, envs, args...)
	})

	t.Run("values from args", func(t *testing.T) {
		args := []string{"--pg.url=postgres://postgres:postgres@localhost:5432/cdt_test"}
		mockArgs(func() {
			pgParams, err := preparePGParams(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, pgParams)
			assert.Equal(t, "postgres://postgres:postgres@localhost:5432/cdt_test", pgParams.ConnStr)
		}, nil, args...)
	})
}

func mockArgs(f func(), vars map[string]string, args ...string) {
	oldEnv := map[string]string{}

	for k, v := range vars {
		oldEnv[k] = os.Getenv(k)
		os.Setenv(k, v)
	}
	flags := pflag.NewFlagSet("TestArguments", pflag.ContinueOnError)
	InitFlags(flags)
	flags.Parse(args)

	f()

	for k, v := range oldEnv {
		os.Setenv(k, v)
	}
}
