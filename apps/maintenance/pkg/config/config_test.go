package config

import (
	"context"
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestS3Params(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("no default creds", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			s3Params, err := prepareS3Config(ctx)
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
			s3Params, err := prepareS3Config(ctx)
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
			s3Params, err := prepareS3Config(ctx)
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

	t.Run("no default creds", func(t *testing.T) {
		args := []string{""}
		mockArgs(func() {
			pgParams, err := preparePostgresConfig(ctx)
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
			pgParams, err := preparePostgresConfig(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, pgParams)
			assert.Equal(t, "postgres://postgres:postgres@localhost:5432/cdt_test", pgParams.ConnStr)
		}, envs, args...)
	})

	t.Run("values from args", func(t *testing.T) {
		args := []string{"--pg.url=postgres://postgres:postgres@localhost:5432/cdt_test"}
		mockArgs(func() {
			pgParams, err := preparePostgresConfig(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, pgParams)
			assert.Equal(t, "postgres://postgres:postgres@localhost:5432/cdt_test", pgParams.ConnStr)
		}, nil, args...)
	})
}

func TestPrepareConfig(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	user := "rootuser"
	passw := "rootpassword"

	// IMPORTANT! test cases could conflict with each other, because they are using global variables (flag values)

	t.Run("valid", func(t *testing.T) {

		t.Run("env", func(t *testing.T) {
			args := []string{"a"}
			env := map[string]string{
				"CRON_SCHEDULE":           "123",
				"MINIO_ENDPOINT":          "localhost:9000",
				"MINIO_ACCESS_KEY_ID":     user,
				"MINIO_SECRET_ACCESS_KEY": passw,
				"POSTGRES_USER":           user,
				"POSTGRES_PASSWORD":       passw,
				"POSTGRES_URL":            "localhost:5432/postgres",
				"POSTGRES_DB":             "postgres",
			}
			mockArgs(func() {
				s := log.CaptureAsString(func() {
					err := PrepareConfig(ctx)
					assert.Nil(t, err)
				})
				assert.NotContains(t, s, "CRON_SCHEDULE")
				assert.NotContains(t, s, "minio.url")
			}, env, args...)
		})

		t.Run("cmd args", func(t *testing.T) {
			args := []string{
				"--run.cron",
				"--minio.url=localhost:9000", "--minio.key=rootuser", "--minio.secret=rootpassword",
				"--pg.url=postgres://postgres:postgres@localhost:5432/postgres"}
			mockArgs(func() {
				s := log.CaptureAsString(func() {
					err := PrepareConfig(ctx)
					assert.Nil(t, err)
				})
				assert.Contains(t, s, "Env variable CRON_SCHEDULE is empty. Default cron schedule is '0 * * * *'")
				assert.NotContains(t, s, "minio.url")
			}, nil, args...)
		})
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
	ParseFlags(flags)

	f()

	for k, v := range oldEnv {
		os.Setenv(k, v)
	}
}
