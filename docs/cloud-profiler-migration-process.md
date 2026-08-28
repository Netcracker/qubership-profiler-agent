# Cloud-profiler migration process

## Table of content

<!-- TOC -->
* [Cloud-profiler migration process](#cloud-profiler-migration-process)
  * [Table of content](#table-of-content)
  * [Overview](#overview)
  * [Installation to separate namespace](#installation-to-separate-namespace)
    * [Pros](#pros)
    * [Cons](#cons)
    * [Installation steps](#installation-steps)
    * [Rollback process](#rollback-process)
  * [Installation to the same namespace](#installation-to-the-same-namespace)
    * [Pros](#pros-1)
    * [Cons](#cons-1)
    * [Installation steps](#installation-steps-1)
    * [Rollback process](#rollback-process-1)
<!-- TOC -->

## Overview

This article describes, how to migrate execution-statistic-collector (ESC) to
[cloud-profiler](https://github.com/Netcracker/qubership-profiler-backend).

## Installation to separate namespace

According to this approach, new cloud-profiler is installed to separate namespace.

### Pros

1. Safer in terms of data;
2. No maintenance time, when profiler information is not collected;
3. ESC functionality is available to work with old data until it's cleaned;

### Cons

1. Required reconfiguration in agents, because profilers endpoints are changed;

### Installation steps

1. Install cloud-profiler to a different namespace than the one where the execution-statistic-collector is installed;  
   **Note**: specify another database names for cloud-profiler to avoid conflicts;
2. Redeploy agents with new profiler endpoints one-by-one;
3. After some time, when ESC data is cleaned, remove its namespace;
4. After some time, when ESC data is cleaned, remove keyspaces/tables in databases:
   * **static-service PG database**, if ESC is deployed in PV mode (`pg.dbName` deploy parameter);
   * **Opensearch indexes**, if `openseach` storage is used in ESC (ESC indexes starts from `ES_INDEX_PREFIX`
parameter value);
   * **Cassandra keyspace**, if `cassandra` storage is used in ESC (`CASSANDRA_KEYSPACE` deploy parameter);

### Rollback process

In case, if cloud-profiler is not installed successfully, it's possible to continue using ESC, until found issue is
resolved;

## Installation to the same namespace

According to this approach, cloud-profiler is installed to the same namespace, where execution-static service works.  
Special job should be installed to the cloud, that removes ESC resources from cloud to avoid conflicts in resource
names;  
It does not remove data related resources (like PVC or databases), so it's possible to return to ESC, if something went
wrong.

### Pros

1. Agents reconfiguration is not needed;

### Cons

1. Profiler functionality is not available during migration;
2. No data migration;

### Installation steps

1. Deploy migration-cleaner to
the same namespace, where ESC is worked;
   * **Note**: do not use clean-install deploy;
1. In case, if migration-cleaner was installed with restricted rights (`privilegedRights=false`), remove ESC cluster
roles and cluster role bindings with commands like:
   * See migration-cleaner deploy with restricted rights
for details;
1. Install cloud-profiler to a different namespace than the one where the execution-statistic-collector is installed;
   * **Note**: do not use the same databases or PVC for cloud-profiler, because it follows data conflicts;
   * **Note**: do not use clean-install deploy;
2. Remove keyspaces/tables in databases:
   * **static-service PG database**, if ESC is deployed in PV mode (`pg.dbName` deploy parameter);
   * **Opensearch indexes**, if `openseach` storage is used in ESC (ESC indexes starts from `ES_INDEX_PREFIX` parameter
value);
   * **Cassandra keyspace**, if `cassandra` storage is used in ESC (`CASSANDRA_KEYSPACE` deploy parameter);
1. Remove `pvc-diag-esc-static-service` PVC, if ESC was installed in PV mode;

### Rollback process

In case, if cloud-profiler or migration-cleaner is not installed successfully, you can reinstall ESC to the old
namespace (in rolling-upgrade mode) to continue using it.
