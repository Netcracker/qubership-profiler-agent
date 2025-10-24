package data

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
)

var Cfg *Config

// ----------------------------------------------------------------------------------

type (
	Prefixes struct {
		NS, Service, Pod string
	}
	OutputDirectories struct {
		OutputDir       string
		ParsedOutputDir string
	}
	Config struct {
		Limit    Limit
		Prefixes Prefixes

		S3            s3.Params
		Postgres      pg.Params
		Parquet       parquet.Params
		ClearPrevious bool
		OnlyParse     bool
		Out           OutputDirectories
	}
)

// ----------------------------------------------------------------------------------

// PrepareConfig parse cmd line options, fail with Panic if something invalid
func PrepareConfig(ctx context.Context) (*Config, error) {
	var err error
	Cfg, err = prepareConfig(ctx)
	return Cfg, err
}

func prepareConfig(ctx context.Context) (*Config, error) {
	limit, err := prepareLimit(ctx)
	if err != nil {
		return nil, err
	}

	prefixes, err := preparePrefixes(ctx)
	if err != nil {
		return nil, err
	}

	s3Params, err := prepareS3Params(ctx)
	if err != nil {
		return nil, err
	}

	pgParams, err := preparePGParams(ctx)
	if err != nil {
		return nil, err
	}

	outputDirs, err := prepareOutputDirs(ctx)
	if err != nil {
		return nil, err
	}

	cfg := Config{
		Limit:         *limit,
		Prefixes:      *prefixes,
		S3:            *s3Params,
		Postgres:      *pgParams,
		Parquet:       parquet.DefaultParams,
		ClearPrevious: *clearPrevious,
		OnlyParse:     *onlyParse,
		Out:           *outputDirs,
	}

	if !cfg.OnlyParse {
		log.Info(ctx, "Cloud: %v", cfg.Limit.Cloud())
		log.Info(ctx, "%d calls per 5 min", cfg.Limit.Calls)
		log.Info(ctx, "Time range: %v", cfg.Limit.Range.String())
		log.Info(ctx, "Output with Parquet files: %v", cfg.Out.OutputDir)
	}
	return &cfg, nil
}

func prepareLimit(ctx context.Context) (*Limit, error) {
	if *numberOfNS <= 0 || *numberOfServices <= 0 || *numberOfPods <= 0 {
		return nil, fmt.Errorf("the number of namespaces, services and pods must be greater than zero")
	}

	timeRange, err := prepareTimeRange(ctx)
	if err != nil {
		return nil, err
	}

	return common.Ref(Limit{
		NS:       *numberOfNS,
		Services: *numberOfServices,
		Pods:     *numberOfPods,
		Range:    *timeRange,
		Calls:    *numberOfCalls,
	}.Fix()), nil
}

func prepareTimeRange(ctx context.Context) (*Range, error) {
	tz := time.UnixMilli(0)
	var err error
	if *onlyParse {
		return &Range{
			StartDate:    tz,
			EndDate:      tz,
			HourDateTime: tz,
			HoursCount:   1,
		}, nil
	}

	start := time.Now().UTC()
	if *startDate != "" {
		start, err = common.ParseDate(*startDate)
		if err != nil {
			log.Error(ctx, err, "Error parsing startdate: %s", *startDate)
			return nil, fmt.Errorf("invalid date format for startdate, must be in the format yyyy/mm/dd")
		}
	}

	end := time.Now().UTC()
	if *endDate != "" {
		end, err = common.ParseDate(*endDate)
		if err != nil {
			log.Error(ctx, err, "Error parsing enddate: %s", *endDate)
			return nil, fmt.Errorf("invalid date format for enddate, must be in the format yyyy/mm/dd")
		}
	}

	if start.After(end) {
		return nil, fmt.Errorf("start date must be before the end date")
	}

	var hourTime = time.UnixMilli(0)
	if *hourDatetime != "" {
		hourTime, err = common.ParseHourTime(*hourDatetime)
		if err != nil {
			log.Error(ctx, err, "Error parsing starttime: %s", *hourDatetime)
			return nil, fmt.Errorf("invalid time format for starttime, must be in the format yyyy/mm/dd/hh")
		}
	}

	timeRange := Range{
		StartDate:    start,
		EndDate:      end,
		HourDateTime: hourTime,
		HoursCount:   *hoursCount,
	}

	if !timeRange.IsDatesValid() {
		return nil, fmt.Errorf("too long time period (%v hours), must be not greater than 31*24 hours", timeRange.Count())
	}

	if !timeRange.IsHoursValid() {
		return nil, fmt.Errorf("invalid hours count (%v hours), must be between 0 and 4 hours", timeRange.HoursCount)
	}

	return &timeRange, nil
}

func preparePrefixes(_ context.Context) (*Prefixes, error) {
	return &Prefixes{*nsPrefix, *servicePrefix, *podPrefix}, nil
}

func prepareS3Params(_ context.Context) (*s3.Params, error) {
	bucket := *minioBucket
	if len(bucket) == 0 {
		bucket = DefaultBucketName
	}

	s3params := &s3.Params{
		Endpoint:        *minioEndpoint,
		AccessKeyID:     *minioAccessKey,
		SecretAccessKey: *minioSecretKey,
		InsecureSSL:     *minioInsecureSSL,
		UseSSL:          *minioUseSSL,
		CAFile:          *minioPathToCA,
		Region:          "",
		BucketName:      bucket,
		ObjectLocking:   false,
	}

	if s3params.IsEmpty() {
		return nil, fmt.Errorf("no credentials for S3. " +
			"In order to upload data to S3 provide minio.url/minio.key/minio.secret cli arguments" +
			" or env variables MINIO_ENDPOINT/MINIO_ACCESS_KEY_ID/MINIO_SECRET_ACCESS_KEY")
	}

	return s3params, nil
}

func preparePGParams(_ context.Context) (*pg.Params, error) {
	connStr := *pgConnectionUrl

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

	return &pg.Params{
		ConnStr: connStr,
		SSLMode: *pgSslMode,
		CAFile:  *pgPathToCA,
	}, nil
}

func prepareOutputDirs(_ context.Context) (*OutputDirectories, error) {
	return &OutputDirectories{
		"output",
		"output/parsed",
	}, nil
}
