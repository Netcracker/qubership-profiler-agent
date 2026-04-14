# Installation

## Prerequisites

- Kubernetes 1.25+;
- Shared (ReadWriteMany) persistent storage or a pre-existing PV;

## Parameters

| Field                   | Description                                                          | Scheme                                    |
|-------------------------|----------------------------------------------------------------------|-------------------------------------------|
| cloud.dumpsStorage      | Dumps storage settings                                               | \*[Dumps Storage](#dumps-storage)         |
| dumpsCollector          | Dumps collector service settings                                     | \*[Dumps Collector](#dumps-collector)     |

### Dumps Storage

| Field              | Description                                                                                                                                                      | Scheme  |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| emptydir           | Deploy on ephemeral (EmptyDir) storage. For dev/testing only. Default: `false`                                                                                   | boolean |
| name               | Pre-existing PV name to mount. Mutually exclusive with `storageClassName`. Default: none                                                                         | string  |
| storageClassName   | Storage class name for dynamic PVC provisioning. Mutually exclusive with `name`. Overridden by `STORAGE_RWX_CLASS` when set. Default: none                                        | string  |
| size               | PVC capacity. Default: `1Gi`                                                                                                                                     | string  |
| accessMode         | PVC access mode. Use `ReadWriteMany` for multi-replica deployments. Default: `ReadWriteOnce`                                                                     | string  |
| hoursArchiveAfter  | Hours after which collected data is compressed into ZIP archives. Default: `2`                                                                                   | integer |
| daysDeleteAfter    | Days after which archived data is removed. Default: `14`                                                                                                         | integer |
| maxHeapDumpsPerPod | Maximum number of heap dumps retained per pod. Default: `10`                                                                                                     | integer |
| host               | HTTP storage proxy host. When set, diagnostic data is forwarded instead of stored on PV. Default: `http://esc-collector-service:8080`                            | string  |

### Dumps Collector

| Field                    | Description                                                                        | Scheme                                                                                                                         |
|--------------------------|------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| name                     | Name for `app:` labels. Default: `cloud-profiler-dumps-collector`                  | string                                                                                                                         |
| image                    | Docker image for dumps-collector                                                   | string                                                                                                                         |
| replicas                 | Number of replicas. Default: `1`                                                   | integer                                                                                                                        |
| securityContext          | Pod security context. Empty by default                                             | \*[v1.PodSecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podsecuritycontext-v1-core)     |
| containerSecurityContext | Container security context. Empty by default                                       | \*[v1.SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)           |
| priorityClassName        | Pod priority class name. Empty by default                                          | string                                                                                                                         |
| resources                | Pod resource quotas. Empty by default                                              | \*[v1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core) |
| annotations              | Additional annotations. Empty by default                                           | object                                                                                                                         |
| labels                   | Additional labels. Empty by default                                                | object                                                                                                                         |
| monitoring               | Monitoring settings                                                                | \*[Monitoring](#monitoring)                                                                                                    |

### Monitoring

| Field    | Description                                | Scheme  |
|----------|--------------------------------------------|---------|
| enabled  | Enable platform monitoring                 | boolean |
| interval | Metrics scrape interval. Default: `30s`    | string  |

### Cloud integration parameters

| Field                              | Description                                                                                                                                                        | Scheme  |
|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| STORAGE_RWX_CLASS                  | Shared (ReadWriteMany) storage class name for the dumps PVC. Takes precedence over `cloud.dumpsStorage.storageClassName` when `global.cloudIntegrationEnabled` is `true`. Default: none | string  |
| global.cloudIntegrationEnabled     | Enables cloud integration mode. When `true`, `STORAGE_RWX_CLASS` takes precedence over `cloud.dumpsStorage.storageClassName`. Default: `false`                    | boolean |
