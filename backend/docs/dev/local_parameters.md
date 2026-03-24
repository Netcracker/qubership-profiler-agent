# Parameters for local run

## Environment Variables

To ensure proper functioning of the application, certain configuration data must be passed in environment variables.
All variables are listed in the table below:

| Variable Name           | Mandatory | Default value | Description                                                     |
| ----------------------- | --------- | ------------- | --------------------------------------------------------------- |
| LOG_LEVEL               | FALSE     | info          | level of logs                                                   |
| MINIO_ENDPOINT          | TRUE      | \-            | Url to Minio instance without protocol. Example: localhost:9001 |
| MINIO_ACCESS_KEY_ID     | TRUE      | \-            | Minio access key                                                |
| MINIO_SECRET_ACCESS_KEY | TRUE      | \-            | Minio secret key                                                |
| POSTGRES_USER           | TRUE      | \-            | Postgres user                                                   |
| POSTGRES_PASSWORD       | TRUE      | \-            | Postgres user password                                          |
| POSTGRES_URL            | TRUE      | \-            | Url to Postgres instance. Example: localhost:5432               |
| POSTGRES_DB             | TRUE      | \-            | Database name                                                   |
| CRON_SCHEDULE           | FALSE     | 0 \* \* \* \* | cron schedule expressions                                       |

## CLI arguments

| Argument           | Default                                                                   | Description                                                                                                       |
| ------------------ | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `--help`           |                                                                           | How to use tool                                                                                                   |
| `--pg.url`         | `postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_URL/$POSTGRES_DB` | Full connection string (with creds) to Postgres instance                                                          |
| `--pg.ssl_mode`    | `prefer`                                                                   | SSL mode for PG connections. Possible values: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `--pg.ca_file`     | ""                                                                        | Path to custom CA certificate for PG                                                                              |
| `--minio.url`      | `$MINIO_ENDPOINT`                                                         | Url to Minio instance                                                                                             |
| `--minio.key`      | `$MINIO_ACCESS_KEY_ID`                                                    | Minio access key                                                                                                  |
| `--minio.secret`   | `$MINIO_SECRET_ACCESS_KEY`                                                | Minio secret key                                                                                                  |
| `--minio.bucket`   | `$MINIO_BUCKET`                                                           | Bucket in Minio                                                                                                   |
| `--minio.insecure` | `false`                                                                   | Use flag insecure for Minio                                                                                       |
| `--minio.use_ssl`  | `false`                                                                   | Use SSL access for Minio                                                                                          |
| `--minio.ca_file`  | ""                                                                        | Path to custom CA certificate for Minio                                                                           |
| `--run.cron`       | `false`                                                                   | Run maintenance job with cron                                                                                     |
| `--run.config`     |                                                                           | Job configuration file location. See [job configuration](./../public/job_configuration.md) for details            |
