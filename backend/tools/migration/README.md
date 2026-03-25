# migration cleaner

## Table of Content

<!-- TOC -->
* [migration cleaner](#migration-cleaner)
  * [Table of Content](#table-of-content)
  * [Overview](#overview)
    * [Resources to remove](#resources-to-remove)
    * [Out of Scope](#out-of-scope)
    * [Rollback process](#rollback-process)
    * [Why clean-install deploy is not enough?](#why-clean-install-deploy-is-not-enough)
  * [Installation](#installation)
    * [Deploy with restricted access](#deploy-with-restricted-access)
  * [Outside the cluster run](#outside-the-cluster-run)
<!-- TOC -->

## Overview

This tool is used to clean old profiler ESC, before new profiler ([cloud-profiler](https://github.com/Netcracker/qubership-profiler-backend))
installation, in case, if they are deployed in the same namespace.

Migration-cleaner should be deployed to the same namespace, where ESC components are deployed, using rolling-update job.

This component is deployed as pre-hook job. It uses special label (by default `app.kubernetes.io/part-of=esc`),
to detect resources, related to ESC, and remove them from cluster.

Then, if job is finished, cloud-profiler should be deployed.

**Note**: migration from ESC to cloud-profiler is not continuous, so the profiler is unavailable and does not work
during this process.

### Resources to remove

Migration-cleaner can remove following resources:

* From used namespace:
  * Deployments;
  * Services;
  * Ingresses;
  * ConfigMaps;
  * Secrets;
  * Roles;
  * Role-bindings;
  * Service-accounts;
  * Grafana dashboards;
  * Service monitors;
  * Cert-manager certificates;
* No namespaced resources:
  * Cluster roles;
  * Cluster role-bindings;

### Out of Scope

This component removes only kubernetes resources, but does not clear ESC data from databases (Opensearch,
Cassandra, Postgres, etc.) and PV (if ESC is deployed using PV mode)

All databases and PV should be cleared manually, **after** successful cloud-profiler installation.

**Note**: it's not recommended to use the same databases for cloud-profiler, because data migration is not
supported and such solution follows to data inconsistency.

### Rollback process

In case, if migration-cleaner fails or cloud-profiler deploy is unsuccessful, and it's not possible to resolve this
issue immediately, you can return to the ESC profiler without losing data.  
For that you should deploy ESC to this namespace in rolling-update mode.  

### Why clean-install deploy is not enough?

Clean-install deploy removes everything, from used namespace, including PVC, that follows losing dump files and
data inconsistency in case, if cloud-profiler deploy fails and rollback is needed.

## Installation

Migration-cleaner should be deployed as part of cloud-profiler, and runs as pre-hook job.

Installation process for migration-cleaner is available [here](documentation/public/installation.md).

### Deploy with restricted access

In case, if app-deployer does not have cluster-admin rights, migration-cleaner should be deployed with
`privilegedRights=false`.

If privileged rights are disabled, migration-cleaner is deployed without cluster role and cluster role-binding
and does not remove ESC non-namespaced resources (cluster roles and cluster roles-entities).

For this reason, in this mode, those entities should be removed manually after cloud-profiler installation.
Resources, that should be removed, can be taken using following commands:

* For cluster roles:

  ```bash
  kubectl get cluster roles -l="app.kubernetes.io/part-of=esc"
  ```
  
* For cluster role-bindings:

  ```bash
  kubectl get clusterrolebindings -l="app.kubernetes.io/part-of=esc"
  ```

## Outside the cluster run

migration-cleaner can be run outside the cluster, for example for dev cases.

To run it outside the cluster, kubeconfig to cluster should be provided. User, configured in this config, should
have access, to [ESC resources](#resources-to-remove).

Migration-cleaner can be configured using environment variables. The whole list:

| Environment       | Description                                                                                                                                             | Default value                   |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| KUBECONFIG        | The path to kubeconfig for kube-client. If empty, in-cluster kube-client mode is used (worked for inside cluster run)                                   |                                 |
| NAMESPACE         | Namespace, where ESC is deployed                                                                                                                        | `profiler`                      |
| ESC_LABEL         | The label, to detect ESC entities                                                                                                                       | `app.kubernetes.io/part-of=esc` |
| PRIVILEGED_RIGHTS | Should be true, if kubeconfig user has access to cluster resources. See [deploy with restricted access](#deploy-with-restricted-access) for details | true                            |
