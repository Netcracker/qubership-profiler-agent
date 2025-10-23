# Installation

## Prerequisites

- Kubernetes 1.25+;
- S3 storage;
- PostgreSQL 14+;

## Capacity planning

Maintenance job has different CPU/memory recommendations depend on the load on the cluster:

- For maintenance job itself:

| Profile  | Recommended on load     | CPU requests | CPU limits | Memory requests | Memory limits |
|----------|-------------------------|--------------|------------|-----------------|---------------|
| `small`  | not more than `50` pods | 100m         | 200m       | 75Mi            | 160Mi         |
| `medium` | `200-400` pods          | 100m         | 200m       | 75Mi            | 160Mi         |
| `large`  | `500+` pods             | 100m         | 200m       | 75Mi            | 160Mi         |

- For maintenance migrate schema job:

| Profile  | Recommended on load     | CPU requests | CPU limits | Memory requests | Memory limits |
|----------|-------------------------|--------------|------------|-----------------|---------------|
| `small`  | not more than `50` pods | 100m         | 200m       | 75Mi            | 160Mi         |
| `medium` | `200-400` pods          | 100m         | 200m       | 75Mi            | 160Mi         |
| `large`  | `500+` pods             | 100m         | 200m       | 75Mi            | 160Mi         |

By default, maintenance job installs with `small` profile,
but it can be changed in deploy parameters or using resource-profiles in deploy job.

## Parameters

| Field          | Description                                                                                                                                 | Scheme                                |
|----------------|---------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| cloud.s3       | Storage settings                                                                                                                            | \*[Storage](#storage)                 |
| cloud.postgres | PG settings                                                                                                                                 | \*[PG](#postgres)                     |
| profile        | Resource profile for maintenance job. Supported values: `small`, `medium`, `large`. See [Capacity planning](#capacity-planning) for details | string                                |
| maintenanceJob | Maintenance job settings                                                                                                                    | \*[Maintenance Job](#maintenance-job) |

### Storage

| Field     | Description                                                                                                               | Scheme                        |
|-----------|---------------------------------------------------------------------------------------------------------------------------|-------------------------------|
| endpoint  | The endpoint for s3 storage. The default value is calculated from [infra passport parameters](#infra-passport-parameters) | string                        |
| accessKey | The access key for s3 storage. The default value is taken from [infra passport parameters](#infra-passport-parameters)    | string                        |
| secretKey | The secret key for s3 storage. The default value is taken from [infra passport parameters](#infra-passport-parameters)    | string                        |
| bucket    | The bucket name for s3 storage. Default value is `profiler`                                                               | string                        |
| tls       | Tls settings for s3 storage                                                                                               | \*[Storage TLS](#storage-tls) |

### Storage TLS

| Field    | Description                                                         | Scheme  |
|----------|---------------------------------------------------------------------|---------|
| insecure | Use insecure access to the s3 storage. The default value is `false` | boolean |
| useSSL   | Use SSL to connect to the s3 storage. The default value is `true`   | boolean |
| ca       | Custom CA certificate. Empty by default                             | string  |

### Postgres

| Field    | Description                                                                                                         | Scheme                    |
|----------|---------------------------------------------------------------------------------------------------------------------|---------------------------|
| host     | The host for PG server. The default value is taken from [infra passport parameters](#infra-passport-parameters)     | string                    |
| port     | The port for PG server. The default value is taken from [infra passport parameters](#infra-passport-parameters)     | string                    |
| username | The username for PG server. The default value is taken from [infra passport parameters](#infra-passport-parameters) | string                    |
| password | The password for PG server. The default value is taken from [infra passport parameters](#infra-passport-parameters) | string                    |
| dbName   | The PG DB name. Default value is used pg username                                                                   | string                    |
| tls      | Tls settings for PG server                                                                                          | \*[PG TLS](#postgres-tls) |

### Postgres TLS

| Field   | Description                                                                                                                            | Scheme |
|---------|----------------------------------------------------------------------------------------------------------------------------------------|--------|
| sslMode | The PG SSL mode. Should be one of `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. The default value is `prefer` | string |
| ca      | Custom CA certificate. Empty by default                                                                                                | string |

### Maintenance job

| Field                    | Description                                                                         | Scheme                                                                                                                         |
|--------------------------|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| name                     | Provide a name for `app:` labels. Default value is `cloud-profiler-maintenance-job` | string                                                                                                                         |
| image                    | Docker image to use for go-profiles-collector deployment                            | string                                                                                                                         |
| jobConfig                | [Job configuration](job_configuration.md) settings. Empty by default                | \*[Job Configuration](./job_configuration.md#configuration-format)                                                             |
| cron                     | The cron to run maintenance cronjob. Default value is `0 * * * *`                   | string                                                                                                                         |
| log.level                | Log level. Default value is `info`                                                  | string                                                                                                                         |
| securityContext          | Defines privilege and access control settings for a Pod. Empty by default           | \*[v1.PodSecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podsecuritycontext-v1-core)     |
| containerSecurityContext | Defines privilege and access control settings for a Container. Empty by default     | \*[v1.SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)           |
| priorityClassName        | Assigned to the Pods to prevent them from evicting. Empty by default                | string                                                                                                                         |
| resources                | Assigned to the Pods resource quotas. Default value depends on `profile` parameter  | \*[v1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core) |
| annotations              | Assigned additional annotations. Empty by default                                   | object                                                                                                                         |
| labels                   | Assigned additional labels. Empty by default                                        | object                                                                                                                         |
| serviceAccount           | Service account settings                                                            | \*[Service Account](#service-account)                                                                                          |
| migrateSchema            | Migrate schema job settings                                                         | \*[Migrate schema Job](#migrate-schema-job)                                                                                    |

### Service account

| Field       | Description                                                                         | Scheme  |
|-------------|-------------------------------------------------------------------------------------|---------|
| name        | Provide a name for `app:` labels. Default value is `cloud-profiler-maintenance-job` | string  |
| install     | If service account should be installed. `true` by default                           | boolean |
| annotations | Assigned additional annotations. Empty by default                                   | object  |
| labels      | Assigned additional labels. Empty by default                                        | object  |

### Migrate schema Job

| Field          | Description                                                                                        | Scheme                                                                                                                         |
|----------------|----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| enabled        | If migrate schema job is enabled. `true` by default                                                | boolean                                                                                                                        |
| name           | Provide a name for `app:` labels. Default value is `cloud-profiler-maintenance-job-migrate-schema` | string                                                                                                                         |
| image          | Docker image to use for go-profiles-collector deployment                                           | string                                                                                                                         |
| enabled        | If migrate schema job is enabled. `true` by default                                                | boolean                                                                                                                        |
| log.level      | Log level. Default value is `info`                                                                 | string                                                                                                                         |
| resources      | Assigned to the Pods resource quotas. Default value depends on `profile` parameter                 | \*[v1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core) |
| annotations    | Assigned additional annotations. Empty by default                                                  | object                                                                                                                         |
| labels         | Assigned additional labels. Empty by default                                                       | object                                                                                                                         |
| serviceAccount | Migrate schema service account settings                                                            | \*[Migrate schema Service Account](#migrate-schema-service-account)                                                            |

### Migrate schema service account

| Field       | Description                                                                                        | Scheme  |
|-------------|----------------------------------------------------------------------------------------------------|---------|
| name        | Provide a name for `app:` labels. Default value is `cloud-profiler-maintenance-job-migrate-schema` | string  |
| install     | If service account should be installed. `true` by default                                          | boolean |
| annotations | Assigned additional annotations. Empty by default                                                  | object  |
| labels      | Assigned additional labels. Empty by default                                                       | object  |

### Infra passport parameters

| Field                         | Description                                           | Scheme |
|-------------------------------|-------------------------------------------------------|--------|
| INFRA_POSTGRES_HOST           | The host for PG server (infra passport parameter)     | string |
| INFRA_POSTGRES_PORT           | The port for PG server (infra passport parameter)     | string |
| INFRA_POSTGRES_ADMIN_USERNAME | The username for PG server (infra passport parameter) | string |
| INFRA_POSTGRES_ADMIN_PASSWORD | The password for PG server (infra passport parameter) | string |
| INFRA_S3_MINIO_ENDPOINT       | MinIO S3 storage endpoint                             | string |
| INFRA_S3_MINIO_ACCESSKEY      | The MinIO access key                                  | string |
| INFRA_S3_MINIO_SECRETKEY      | The MinIO secret key                                  | string |
