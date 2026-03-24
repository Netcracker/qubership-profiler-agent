package config

import (
	"os"

	"github.com/spf13/pflag"
)

// ----------------------------------------------------------------------------------
// command line options
var (
	cronRun           bool
	jobConfigLocation string
	pgConnectionUrl   string
	pgSslMode         string
	pgPathToCA        string
	minioEndpoint     string
	minioAccessKey    string
	minioSecretKey    string
	minioBucket       string
	minioInsecureSSL  bool
	minioUseSSL       bool
	minioPathToCA     string
)

func InitFlags(flags *pflag.FlagSet) {
	flags.Bool("run.cron", false, "Run comparator with cron")
	flags.String("run.config", "", "Location of config with time ranges for different jobs")
	flags.String("minio.url", os.Getenv("MINIO_ENDPOINT"), "Url to Minio instance")
	flags.String("minio.key", os.Getenv("MINIO_ACCESS_KEY_ID"), "Minio access key")
	flags.String("minio.secret", os.Getenv("MINIO_SECRET_ACCESS_KEY"), "Minio secret key")
	flags.String("minio.bucket", os.Getenv("MINIO_BUCKET"), "Bucket in Minio")
	flags.Bool("minio.insecure", false, "Use flag insecure for Minio")
	flags.Bool("minio.use_ssl", false, "Use ssl access for Minio")
	flags.String("minio.ca_file", "", "Path to custom CA certificate for Minio")
	InitPGFlags(flags)
}

func InitPGFlags(flags *pflag.FlagSet) {
	flags.String("pg.url", "", "Full connection string to Postgres instance")
	flags.String("pg.ssl_mode", "prefer", "SSL mode for PG connections. Possible values: disable, allow, prefer, require, verify-ca, verify-full")
	flags.String("pg.ca_file", "", "Path to custom CA certificate for PG")
}

func ParseFlags(flags *pflag.FlagSet) {
	cronRun, _ = flags.GetBool("run.cron")
	jobConfigLocation, _ = flags.GetString("run.config")
	minioEndpoint, _ = flags.GetString("minio.url")
	minioAccessKey, _ = flags.GetString("minio.key")
	minioSecretKey, _ = flags.GetString("minio.secret")
	minioBucket, _ = flags.GetString("minio.bucket")
	minioInsecureSSL, _ = flags.GetBool("minio.insecure")
	minioUseSSL, _ = flags.GetBool("minio.use_ssl")
	minioPathToCA, _ = flags.GetString("minio.ca_file")
	ParsePGFlags(flags)
}

func ParsePGFlags(flags *pflag.FlagSet) {
	pgConnectionUrl, _ = flags.GetString("pg.url")
	pgSslMode, _ = flags.GetString("pg.ssl_mode")
	pgPathToCA, _ = flags.GetString("pg.ca_file")
}
