# Installation

## Prerequisites

- Kubernetes 1.25+;

## Capacity planning

Migration-cleaner contains one job, that runs during deploy. This job is not dependent on cluster load, for this
reason this component does not need profiles support.

| Component             | CPU requests | CPU limits | Memory requests | Memory limits |
|-----------------------|--------------|------------|-----------------|---------------|
| migration-cleaner job | 100m         | 200m       | 75Mi            | 160Mi         |

## Parameters

| Field            | Description                                                                                                                                                                 | Scheme                |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|
| privilegedRights | Should be enabled, if job has cluster admin rights. See [deploy with restricted access](../../README.md#deploy-with-restricted-access) for details. Default value is `true` | boolean               |
| cleaner          | Migration-cleaner settings                                                                                                                                                  | \*[Cleaner](#cleaner) |

### Cleaner

| Field                    | Description                                                                                                 | Scheme                                                                                                                         |
|--------------------------|-------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| enabled                  | Enable migration-cleaner. Default value is `true`                                                           | boolean                                                                                                                        |
| name                     | Provide a name for `app:` labels. Default value is `cloud-profiler-migration-cleaner`                       | string                                                                                                                         |
| image                    | Docker image to use for migration-cleaner job                                                               | string                                                                                                                         |
| escLabelSelector         | Specify label selector to detect ESC resources. Default value is `app.kubernetes.io/part-of=esc`            | string                                                                                                                         |
| log.level                | Log level for migration-cleaner job. Default value is `info`                                                | string                                                                                                                         |
| securityContext          | Defines privilege and access control settings for a Pod. Empty by default                                   | \*[v1.PodSecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podsecuritycontext-v1-core)     |
| containerSecurityContext | Defines privilege and access control settings for a Container. Empty by default                             | \*[v1.SecurityContext](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#securitycontext-v1-core)           |
| priorityClassName        | Assigned to the Pods to prevent them from evicting. Empty by default                                        | string                                                                                                                         |
| resources                | Assigned to the Pods resource quotas. See default values in [capacity planning](#capacity-planning) section | \*[v1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#resourcerequirements-v1-core) |
| annotations              | Assigned additional annotations. Empty by default                                                           | object                                                                                                                         |
| labels                   | Assigned additional labels. Empty by default                                                                | object                                                                                                                         |
| serviceAccount           | Service account settings                                                                                    | \*[Service Account](#service-account)                                                                                          |

### Service account

| Field       | Description                                                                            | Scheme |
|-------------|----------------------------------------------------------------------------------------|--------|
| name        | Provide a name for `app:` labels. Default value is `cloud-profiler-migration-cleaner`  | string |
| annotations | Assigned additional annotations. Empty by default                                      | object |
| labels      | Assigned additional labels. Empty by default                                           | object |
