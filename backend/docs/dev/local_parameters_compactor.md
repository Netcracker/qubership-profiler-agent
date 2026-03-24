# Parameter for local run

## Environment Variables

To ensure proper functioning of the application, certain configuration data must be passed in environment variables.
All variables are listed in the table below:

| Variable Name           | Mandatory | Default value | Description                                                                                                                                           |
| ----------------------- | --------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| MINIO_ENDPOINT          | TRUE      | \-            | Url to Minio instance without protocol. Example: localhost:9001                                                                                       |
| MINIO_ACCESS_KEY_ID     | TRUE      | \-            | Minio access key                                                                                                                                      |
| MINIO_SECRET_ACCESS_KEY | TRUE      | \-            | Minio secret key                                                                                                                                      |
| POSTGRES_USER           | TRUE      | \-            | Postgres user                                                                                                                                         |
| POSTGRES_PASSWORD       | TRUE      | \-            | Postgres user password                                                                                                                                |
| POSTGRES_URL            | TRUE      | \-            | Url to Postgres instance. Example: localhost:5432                                                                                                     |
| POSTGRES_DB             | TRUE      | \-            | Database name                                                                                                                                         |
| OUTPUT_DIR              | FALSE     | ./output/     | Local directory where parquet files will be stored                                                                                                    |
| CRON_SCHEDULE           | FALSE     | 7 \* \* \* \* | cron schedule expressions                                                                                                                             |
| IMPORTANT_PARAMS        | FALSE     |               | List of parameters that should be saved as an inverted index, parameters are specified in a string representation separated by commas without spaces. |
| LOG_LEVEL               | FALSE     | info          | level of logs                                                                                                                                         |

## CLI arguments

| Argument                 | Default                                                                   | Description                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `--help`                 |                                                                           | How to use tool                                                                                                   |
| `--bind_metrics_address` | `0.0.0.0:6060`                                                            | Address to bind metric server                                                                                     |
| `--pg.url`               | `postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_URL/$POSTGRES_DB` | Full connection string (with creds) to Postgres instance                                                          |
| `--pg.ssl_mode`          | `prefer`                                                                   | SSL mode for PG connections. Possible values: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `--pg.ca_file`           | ""                                                                        | Path to custom CA certificate for PG                                                                              |
| `--minio.url`            | `$MINIO_ENDPOINT`                                                         | Url to Minio instance                                                                                             |
| `--minio.key`            | `$MINIO_ACCESS_KEY_ID`                                                    | Minio access key                                                                                                  |
| `--minio.secret`         | `$MINIO_SECRET_ACCESS_KEY`                                                | Minio secret key                                                                                                  |
| `--minio.bucket`         | `$MINIO_BUCKET`                                                           | Bucket in Minio                                                                                                   |
| `--minio.insecure`       | `false`                                                                   | Use flag insecure for Minio                                                                                       |
| `--minio.use_ssl`        | `false`                                                                   | Use SSL access for Minio                                                                                          |
| `--minio.ca_file`        | ""                                                                        | Path to custom CA certificate for Minio                                                                           |
| `--run.cron`             | `false`                                                                   | Run compactor with cron                                                                                           |
| `--run.time`             |                                                                           | Run compactor process for specific time. Format is `yyyy/mm/dd/hh`                                                |
| `--run.status`           |                                                                           | Run compactor process for specific table statuses (by default runs for `ready` status)                            |
