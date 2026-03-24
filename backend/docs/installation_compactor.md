# Installation

## Prerequisites

- Kubernetes 1.25+;
- S3 storage;
- PostgreSQL 14+;

## Capacity planning

Compactor has different CPU/memory recommendations depend on the load on the cluster:

| Profile  | Recommended on load     | CPU requests | CPU limits | Memory requests | Memory limits |
|----------|-------------------------|--------------|------------|-----------------|---------------|
| `small`  | not more than `50` pods | 100m         | 200m       | 75Mi            | 160Mi         |
| `medium` | `200-400` pods          | 100m         | 200m       | 75Mi            | 160Mi         |
| `large`  | `500+` pods             | 100m         | 200m       | 75Mi            | 160Mi         |

By default, compactor installs with `small` profile,
but it can be changed in deploy parameters or using resource-profiles in deploy job.

## Parameters

| Field          | Description                                                                                                                           | Scheme                    |
|----------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| cloud.s3       | Storage settings                                                                                                                      | \*[Storage](#storage)     |
| cloud.postgres | Postgres settings                                                                                                                     | \*[PG](#postgres)         |
| profile        | Resource profile for compactor. Supported values: `small`, `medium`, `large`. See [Capacity planning](#capacity-planning) for details | string                    |
| compactor      | Compactor settings                                                                                                                    | \*[Compactor](#compactor) |

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

### Compactor

| Field                    | Description                                                                        | Scheme                                                                                                                         |
|--------------------------|------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| name                     | Provide a name for `app:` labels. Default value is `cloud-profiler-compactor`      | string                                                                                                                         |
| image                    | Docker image to use for go-profiles-collector deployment                           | string                                                                                                                         |
| cron                     | The cron to run maintenance cronjob. Default value is `0 * * * *`                  | string                                                                                                                         |
| log.level                | Log level. Default value is `info`                                                 | string                                                                                                                         |
| securityContext          | Defines privilege and access control settings for a Pod. Empty by default          | \*[v1.PodSecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podsecuritycontext-v1-core)     |
| containerSecurityContext | Defines privilege and access control settings for a Container. Empty by default    | \*[v1.SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)           |
| priorityClassName        | Assigned to the Pods to prevent them from evicting. Empty by default               | string                                                                                                                         |
| resources                | Assigned to the Pods resource quotas. Default value depends on `profile` parameter | \*[v1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core) |
| annotations              | Assigned additional annotations. Empty by default                                  | object                                                                                                                         |
| labels                   | Assigned additional labels. Empty by default                                       | object                                                                                                                         |
| serviceAccount           | Service account settings                                                           | \*[Service Account](#service-account)                                                                                          |
| monitoring               | Monitoring settings                                                                | \*[Monitoring](#monitoring)                                                                                                    |
| pvc                      | PVC settings                                                                       | \*[pvc](#pvc)                                                                                                                  |

### Service account

| Field       | Description                                                                   | Scheme  |
|-------------|-------------------------------------------------------------------------------|---------|
| name        | Provide a name for `app:` labels. Default value is `cloud-profiler-compactor` | string  |
| install     | If service account should be installed. `true` by default                     | boolean |
| annotations | Assigned additional annotations. Empty by default                             | object  |
| labels      | Assigned additional labels. Empty by default                                  | object  |

### Monitoring

| Field    | Description                                | Scheme  |
|----------|--------------------------------------------|---------|
| enabled  | Enabled platform monitoring                | boolean |
| interval | Interval to send metrics. Default is `30s` | string  |

### PVC

| Field       | Description                                                                                                                                                                        | Scheme                                                                                                                                |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| name        | Provide a name for `app:` labels. Default value is `cloud-profiler-compactor-pvc`                                                                                                  | string                                                                                                                                |
| install     | If sPVC should be installed. If `false`, empty dir will be used. `true` by default                                                                                                 | boolean                                                                                                                               |
| annotations | Assigned additional annotations. Empty by default                                                                                                                                  | object                                                                                                                                |
| labels      | Assigned additional labels. Empty by default                                                                                                                                       | object                                                                                                                                |
| spec        | Spec information for PVC. Default value is `{"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}, "storageClassName": "", "volumeMode": "Filesystem"}` | \*[PersistentVolumeClaimSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#persistentvolumeclaimspec-v1-core) |

### Infra passport parameters

| Field                         | Description                                           | Scheme  |
|-------------------------------|-------------------------------------------------------|---------|
| INFRA_POSTGRES_HOST           | The host for PG server (infra passport parameter)     | string  |
| INFRA_POSTGRES_PORT           | The port for PG server (infra passport parameter)     | string  |
| INFRA_POSTGRES_ADMIN_USERNAME | The username for PG server (infra passport parameter) | string  |
| INFRA_POSTGRES_ADMIN_PASSWORD | The password for PG server (infra passport parameter) | string  |
| INFRA_S3_MINIO_ENDPOINT       | MinIO S3 storage endpoint                             | string  |
| INFRA_S3_MINIO_ACCESSKEY      | The MinIO access key                                  | string  |
| INFRA_S3_MINIO_SECRETKEY      | The MinIO secret key                                  | string  |
