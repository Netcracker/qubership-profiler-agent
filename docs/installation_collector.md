This document provides information about the requirements, configuration, and steps to install CDT to an environment.

# Table of Content

<!-- TOC -->
* [Table of Content](#table-of-content)
* [Before you begin](#before-you-begin)
  * [Kubernetes compatibility](#kubernetes-compatibility)
  * [NC Diagnostic Agent](#nc-diagnostic-agent)
  * [Capacity planning](#capacity-planning)
    * [CPU and Memory](#cpu-and-memory)
    * [Storage size](#storage-size)
    * [Deployment profiles](#deployment-profiles)
  * [Storages](#storages)
    * [Cassandra](#cassandra)
    * [OpenSearch](#opensearch)
* [Deployment](#deployment)
  * [Collector service](#collector-service)
  * [UI service](#ui-service)
  * [Static service](#static-service)
  * [Integration tests](#integration-tests)
  * [Pprof collector](#pprof-collector)
* [Smoke test](#smoke-test)
<!-- TOC -->

# Before you begin

* You need storage for CDT: Cassandra (recommended) or OpenSearch/ElasticSearch
* You need namespace in Kubernetes 1.15+ in which CDT will be installed
* NC Diagnostic Agent installed in namespaces with applications, details read in the section
  [NC Diagnostic Agent](#nc-diagnostic-agent).
* Hardware requirements see in [Capacity planning](#capacity-planning)

## Kubernetes compatibility

CDT can be deployed in the following Clouds:

* Kubernetes 1.15+
* OpenShift 4.x

## NC Diagnostic Agent

CDT can receive diagnostic information using two options:

* microservice can push diagnostic data to `nc-diagnostic-agent` and data will proxy to `cdt-collector-service`
* microservice can directly push diagnostic data to `cdt-collector-service`

We usually **recommended** using a `nc-diagnostic-agent` as a proxy because this option has several advantages:

* the address/name of `nc-diagnostic-agent` configured as default values of `NC_DIAGNOSTIC_AGENT_SERVICE`
  so microservice by default will send diagnostic data to this address
* it's a central point of namespace where you can disable sending diagnostic data to CDT

**Warning!** NC Diagnostic Agent should deploy in the application's namespace before deploy application.

NC Diagnostic Agent usually should deploy in each namespace where microservices can collect and send diagnostic data to
CDT.

## Capacity planning

Hardware recommendation depends on a planning load on CDT and number of active pods.

### CPU and Memory

> **Warning!**
>
> Please do not forget to change `JAVA_OPTIONS` parameter and values of `-Xmx`, `-Xms`, `-Xss`
> and other when you want to increase allocated memory for collector or ui services

For small or development environments, you can use default hardware settings (not more than `50` pods):

| Component        | CPU requests | CPU limits | Memory requests | Memory limits |
|------------------|--------------|------------|-----------------|---------------|
| collector-server | 100m         | 2000m      | 550Mi           | 550Mi         |
| ui-service       | 100m         | 2000m      | 650Mi           | 650Mi         |
| static-service   | 100m         | 100m       | 20Mi            | 40Mi          |
| pprof-collector  | 50m          | 100m       | 64Mi            | 256Mi         |

In cases of a high load, when a number of pods are more than `200-400` and these pods generated a lot of requests
recommended requirements can be increased to:

| Component        | CPU requests | CPU limits | Memory requests | Memory limits |
|------------------|--------------|------------|-----------------|---------------|
| collector-server | 1000m        | 3000m      | 3500Mi          | 5000Mi        |
| ui-service       | 1000m        | 2000m      | 2500Mi          | 3500Mi        |
| static-service   | 100m         | 100m       | 64Mi            | 128Mi         |
| pprof-collector  | 50m          | 100m       | 128Mi           | 512Mi         |

For huge environments with `500+` pods, you can use the following hardware resources:

| Component        | Replicas | CPU requests | CPU limits | Memory requests | Memory limits |
|------------------|----------|--------------|------------|-----------------|---------------|
| collector-server | 2        | 1000m        | 3000m      | 3500Mi          | 5000Mi        |
| ui-service       | 1        | 1000m        | 2000m      | 2048Mi          | 4096Mi        |
| static-service   | 1        | 100m         | 100m       | 64Mi            | 128Mi         |
| pprof-collector  | 1        | 100m         | 200m       | 256Mi           | 1024Mi        |

### Storage size

> **Warning!**
>
> Please do not set `LOG_RETENTION_PERIOD` less than the average pod time life. It can lead to incorrect work of
> cleanup procedure, when cleanup will remove the information about pod, although pod is still alive and send data.

Also, CDT can require `200 Mb` storage for each pod for a storage period.
So, for calculated required storage, you can use a formula:

    ```text
    <number of pods> * <restart factor> * 200 Mb = <size for retention period>
    ```

Some explanation for this formula:

* The size `200 Mb` it a maximum size of information that CDT allow to accumulate per pod.
  Can be changed with using parameter `LOG_MAX_SIZE_KB=xxx`.
* Under `<number of pods>` means the number of pods in Cloud for which enabled profiling. Not all your pods in Cloud.
* Under `<restart factor>` means the count of pods restarts for a retention period.

Obviously, for production and for development environments, this factor will be different. It can be calculated as:

    ```text
    max(1, <retention period> / <the average pod time life> ) = <restart factor>
    ```

For example:

* if the average pod time life is `1 day` and the retention period is `14 days`, so the restart factor will `14`
* if the average pod time file is `30 days` and the retention period is `14 days`, to the restart factor will `1`

In development environments, we expect many more restarts than in production

As a result, we can provide some examples.
But please keep in mind, that `200 Mb` is the maximum size of accumulated data.
In fact, the average size of accumulated data can be smaller and in internal environments it sizes about `10-20 Mb`.

* Small development cloud:
  Calculated as:

      ```text
      Number of pods = 50
      Max calls size for pod = 200Mb
      Retention period = 2 weeks
      Restart factor = 14
      ---
      50 pods * 200 Mb * 14 restarts ~= 136 Gb, for 2 weeks  
      ```

* Big development cloud:
  Calculated as:

      ```text
      Number of pods = 400
      Max calls size for pod = 200Mb
      Average call size for pod = 20Mb
      Retention period = 2 weeks
      Restart factor = 14 
      ---
      50 pods * 200 Mb * 14 restarts ~= 1 Tb, max size for 2 weeks
      50 pods * 20 Mb * 14 restarts ~= 100 Gb, average size for 2 weeks
      ```

* Production cloud:
  Calculated as:

      ```text
      Number of pods = 200
      Max calls size for pod = 200Mb
      Retention period = 2 weeks
      Restart factor = 1
      ---
      200 pods * 200 Mb * 1 restart ~= 40 Gb, for 2 weeks
      ```

### Deployment profiles

CDT support and provide some hardware profiles which you can use for deployed and set hardware resources.

> **Note:**
>
> Currently, specified the real values which are used in these deployment profiles.
> But all these values are obsolete and need to be updated.

The profile with name `dev`:

|                   | Replicas | CPU requests | CPU limits | Memory requests | Memory limits |
|-------------------|----------|--------------|------------|-----------------|---------------|
| collector-service | 1        | 100m         | 8000m      | 550Mi           | 550Mi         |
| ui-service        | 1        | 100m         | 4000m      | 650Mi           | 650Mi         |
| static-service    | -        | -            | -          | -               | -             |
| pprof-collector   | -        | -            | -          | -               | -             |

The profile with name `default`:

|                   | Replicas | CPU requests | CPU limits | Memory requests | Memory limits |
|-------------------|----------|--------------|------------|-----------------|---------------|
| collector-service | 1        | 1000m        | 2000m      | 1600Mi          | 1600Mi        |
| ui-service        | 1        | 100m         | 1500m      | 1024Mi          | 1024Mi        |
| static-service    | 1        | 100m         | 300m       | 128Mi           | 256Mi         |
| pprof-collector   | 1        | 50m          | 100m       | 128Mi           | 512Mi         |

The profile with name `heavy`:

|                   | Replicas | CPU requests | CPU limits | Memory requests | Memory limits |
|-------------------|----------|--------------|------------|-----------------|---------------|
| collector-service | 2        | 1000m        | 4000m      | 1600Mi          | 2800Mi        |
| ui-service        | -        | -            | -          | -               | -             |
| static-service    | -        | -            | -          | -               | -             |
| pprof-collector   | 1        | 100m         | 200m       | 256Mi           | 1024Mi        |

where:

* `-` is meaning that for this component there is no such profile

## Storages

CDT requires one of the supported storages:

* Cassandra 3.x or 4.x
* OpenSearch 1.x or 2.x (OpenSearch 2.x supports since version 9.3.2.64)
* ElasticSearch 7.10.x (compatibility with the highest versions didn't verify!)

> NOTE: Pprof collector supports only OpenSearch storage.

Supported Public Cloud storage:

| Cloud storage                               | Status |
|---------------------------------------------|--------|
| Amazon AWS Keyspaces                        | ✓*     |
| Amazon AWS OpenSearch                       | ✓      |
| Azure Managed Instance for Apache Cassandra | ?      |
| Azure Cosmos DB                             | ✗      |

where:

* `✓` - supported
* `✗` - not supported
* `?` - not verified yet

> **Note:**
>
> \* Amazon AWS Keyspaces we now don't recommend using due to potentially high cost.

We strongly recommend using separated instance of storage for all Ops tools.
Do not try to use the same Cassandra or OpenSearch for store business data and operational data.

But you can use the same Cassandra or OpenSearch Jaeger and CDT/ESC data.

There are some reasons why we recommend it:

* Operational tools should not affect business data and work of business applications
* Currently, CDT generates a massive load on storages and can affect business applications

> **Note:**
>
> Since version `9.3.2.31` by default static-service proxy, all payloads to collector-service which handle them
> and store directly to storage.
> Thus, the Persistence Volume (PV) is no longer needed for CDT deploy.

### Cassandra

Currently, CDT stores a lot of data and has non-optimal schema for Cassandra.
Also, CDT executes a lot of reads from Cassandra.

All these reasons lead to the situation that CDT requires quite a performance Cassandra instance.

We recommended using Cassandra single instance or cluster with hardware resources, not less that:

* CPU: 2-4 cores
* Memory: 6-8 Gb

By default, CDT deploy and work with Cassandra without data replication.

So, if you want to use Cassandra cluster with 3 or more nodes, do not forget to specify in deployed parameters:

    ```yaml
    CASSANDRA_REPLICATION: 3
    ```

### OpenSearch

**Supported since:** 9.3.2.55

For working with OpenSearch/ElasticSearch, we recommended using OpenSearch 2.x
(OpenSearch 2.x version supports since 9.3.2.64).

And with resources not less that (per node):

* CPU: 2-4 cores
* Memory: 6-8 Gb

# Deployment

## Collector service

Collector-service is a component which exposes endpoint to receiving calls and other debug information from services and
store received data to storage.

The following configuration options are available:

<!-- markdownlint-disable line-length -->
| Parameter          | Default value                                                                    | Support since | Description                                                                                                                                          |
|--------------------|----------------------------------------------------------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| COLLECTOR_HOST     | {{ .Values.SERVICE_NAME }}-{{ .Values.NAMESPACE }}.{{ .Values.SERVER_HOSTNAME }} | -             | Allow to override Ingress host address for collector-service. By default host will generate with using: `servicer-name + namespace + cloud dns name` |
| MONITORING_ENABLED | true                                                                             | -             | Enable deploy monitoring objects which enable integration with monitoring. In case collector-service it is ServiceMonitor and GrafanaDashboard.      |
<!-- markdownlint-enable line-length -->

**Cleanup**

<!-- markdownlint-disable line-length -->
| Parameter              | Default value | Support since | Description                                                                                                     |
|------------------------|---------------|---------------|-----------------------------------------------------------------------------------------------------------------|
| LOG_RETENTION_PERIOD   | 1209600000    | -             | Retention period for profiler data in milliseconds. Set the `0` value to disable cleanup. (default - `2 weeks`) |
| LOG_MAX_SIZE_KB        | 204800        | -             | Maximum size of profiler logs per POD in KB (default - `200 MB`)                                                |
| STREAM_ROTATION_PERIOD |               | -             |                                                                                                                 |
<!-- markdownlint-enable line-length -->

**Storage: Common**

<!-- markdownlint-disable line-length -->
| Parameter         | Default value          | Support since | Description                                                       |
|-------------------|------------------------|---------------|-------------------------------------------------------------------|
| NUM_IDLE_CLIENTS  | NUM_HEAVY_CLIENTS * 10 | -             | Number of inactive client connections which just keep connections |
| NUM_HEAVY_CLIENTS | 100                    | -             | Number of active client connections which read or write data      |
<!-- markdownlint-enable line-length -->

**Storage: Cassandra**

<!-- markdownlint-disable line-length -->
| Parameter                     | Default value                                      | Support since | Description                                                                                                                                                                                                                                                                                                                                                                             |
|-------------------------------|----------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASSANDRA_HOST                | -                                                  | 9.3.2.29      | Hostname of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                                  |
| CASSANDRA_PORT                | 9042                                               | -             | Port of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                                      |
| CASSANDRA_KEYSPACE            | esc                                                | -             | Name of Cassandra space in cloud                                                                                                                                                                                                                                                                                                                                                        |
| CASSANDRA_DC                  | datacenter1 if connecting to localhost, dc1 if not | 9.3.2.25      | Specify Cassandra datacenter. Added default values for compatibility with default docker image and default cloud distribution.                                                                                                                                                                                                                                                          |
| CASSANDRA_USERNAME            | -                                                  | 9.3.2.29      | Username to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                        |
| CASSANDRA_PASSWORD            | -                                                  | 9.3.2.29      | Password to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                        |
| CASSANDRA_CONSISTENCY         | LOCAL_ONE                                          | 9.3.2.25      | Write consistency level as described in [https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html)                                                                                                                                                                |
| CASSANDRA_REPLICATION         | -                                                  | 9.3.2.38      | Allow change replication strategy from SimpleStrategy (use by default, data write without replication) to NetworkReplicationStrategy (data will write with specified replication factor, for DC specified in `CASSANDRA_DC`). Please pay attention that replication factor more that 1 required Cassandra cluster with the same number of nodes. For example: `CASSANDRA_REPLICATION=3` |
| CASSANDRA_CERT                | -                                                  | 9.3.2.25      | Base64-encoded certificate of Cassandra server to be added to java keystore. For example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html)                                                                                                                         |
| CASSANDRA_CONFIG              | -                                                  | 9.3.2.25      | application.conf file with custom parameters for Cassandra drivers. For example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html) to connect to AWS please use 4.x DataStax Java driver for Apache Cassandra and the SigV4 authentication plugin                   |
| CASSANDRA_REQUEST_TIMEOUT     | 30s                                                | 9.3.2.58      | Timeout of requests to select data from Cassandra                                                                                                                                                                                                                                                                                                                                       |
| CASSANDRA_SCHEMA_INIT_TIMEOUT | 60s                                                | 9.3.2.58      | Timeout to initialize schema in Cassandra                                                                                                                                                                                                                                                                                                                                               |
<!-- markdownlint-enable line-length -->

**Storage: ElasticSearch**

<!-- markdownlint-disable line-length -->
| Parameter   | Default value | Support since | Description                                                                                 |
|-------------|---------------|---------------|---------------------------------------------------------------------------------------------|
| ES_PROTOCOL | -             | 9.3.2.58      | Connection protocol of OpenSearch/ElasticSearch. Available values: http, https.             |
| ES_HOST     | -             | 9.3.2.29      | Host of OpenSearch/ElasticSearch storage in cloud. Ignored if `CASSANDRA_HOST` is specified |
| ES_PORT     | -             | 9.3.2.29      | Port of OpenSearch/ElasticSearch storage in cloud. Ignored if `CASSANDRA_HOST` is specified |
| ES_USERNAME | admin         | 9.3.2.29      | Username to connect to OpenSearch/ElasticSearch                                             |
| ES_PASSWORD | admin         | 9.3.2.29      | Password to connect to OpenSearch/ElasticSearch                                             |
| ES_SHARDS   | -             | 9.3.2.35      | Number of shards in OpenSearch/ElasticSearch                                                |
| ES_REPLICAS | -             | 9.3.2.35      | Number of replicas in OpenSearch/ElasticSearch                                              |
<!-- markdownlint-enable line-length -->

**Agent**

<!-- markdownlint-disable line-length -->
| Parameter                   | Default value       | Support since | Description                                                                                                                                                 |
|-----------------------------|---------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NC_DIAGNOSTIC_MODE          | prod                | 9.3.2.25      | Allow to enable self-profiling of collector-service. Parameter have the same values as in Supporting  profiling and tracing in JAVA service                 |
| NC_DIAGNOSTIC_AGENT_SERVICE | nc-diagnostic-agent | 9.3.2.25      | Allow to override host where agent will send diagnostic information. Parameter have the same values as in Supporting  profiling and tracing in JAVA service |
| ZOOKEEPER_ENABLED           | -                   |               | Allow to enable ZooKeeper integration. ESC Agent can fetch settings from ZooKeeper and even a custom config for agent.                                      |
| ZOOKEEPER_ADDRESS           | -                   |               | Address of ZooKeeper for fetch custom agent's settings.                                                                                                     |
<!-- markdownlint-enable line-length -->

Also, `collector-service` support service-level deployment parameters which can be specified only by using deploy profile:

<!-- markdownlint-disable line-length -->
| Parameter      | Default value                                                                                                                                                  | Support since | Description                                                                                                                                         |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| REPLICAS       | 1                                                                                                                                                              | -             | Number of expected replicas of collector-service                                                                                                    |
| JAVA_OPTIONS   | -Xmx256m -Xms256m -XX:MaxDirectMemorySize=64M -Dhttp.maxConnections=32 -Xss512k -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m -XX:ReservedCodeCacheSize=64M | -             | Allow to specify list of Java arguments which will apply to JVM. For example: /usr/bin/java ${JAVA_OPTIONS} -Djava.security.egd=file:/dev/./urandom |
| CPU_REQUEST    | 100m                                                                                                                                                           | -             | Allow to set CPU request for pod                                                                                                                    |
| MEMORY_REQUEST | 650Mi                                                                                                                                                          | -             | Allow to set Memory request for pod                                                                                                                 |
| CPU_LIMIT      | 4                                                                                                                                                              | -             | Allow to set CPU limits for pod                                                                                                                     |
| MEMORY_LIMIT   | 650Mi                                                                                                                                                          | -             | Allow to set Memory limits for pod                                                                                                                  |
<!-- markdownlint-enable line-length -->

## UI service

UI-service is a service which provides a User Interface for show collected calls and other diagnostic information.

The following configuration options are available:

<!-- markdownlint-disable line-length -->
| Parameter          | Default value                                                                    | Support since | Description                                                                                                                                 |
|--------------------|----------------------------------------------------------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| SKIP_UI            | false                                                                            |               | Allow to skip deploy ui-service.                                                                                                            |
| UI_SERVICE         | {{ .Values.SERVICE_NAME }}-{{ .Values.NAMESPACE }}.{{ .Values.SERVER_HOSTNAME }} | -             | Allow to override Ingress host address for ui-service. By default host will generate with using: servicer-name + namespace + cloud dns name |
| MONITORING_ENABLED | true                                                                             |               | Enable deploy monitoring objects which enable integration with monitoring. In case ui-service it is ServiceMonitor.                         |
<!-- markdownlint-enable line-length -->

**Storage: Common**

<!-- markdownlint-disable line-length -->
| Parameter         | Default value          | Support since | Description                                                       |
|-------------------|------------------------|---------------|-------------------------------------------------------------------|
| NUM_IDLE_CLIENTS  | NUM_HEAVY_CLIENTS * 10 | -             | Number of inactive client connections which just keep connections |
| NUM_HEAVY_CLIENTS | 100                    | -             | Number of active client connections which read or write data      |
<!-- markdownlint-enable line-length -->

**Storage: Cassandra**

<!-- markdownlint-disable line-length -->
| Parameter                     | Default value                                      | Support since | Description                                                                                                                                                                                                                                                                                                                                                                         |
|-------------------------------|----------------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASSANDRA_HOST                | -                                                  | 9.3.2.29      | Hostname of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                              |
| CASSANDRA_PORT                | 9042                                               | -             | Port of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                                  |
| CASSANDRA_KEYSPACE            | esc                                                | -             | Name of Cassandra space in cloud                                                                                                                                                                                                                                                                                                                                                    |
| CASSANDRA_DC                  | datacenter1 if connecting to localhost, dc1 if not | 9.3.2.25      | Specify Cassandra datacenter. Added default values for compatibility with default docker image and default cloud distribution.                                                                                                                                                                                                                                                      |
| CASSANDRA_USERNAME            | -                                                  | 9.3.2.29      | Username to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                    |
| CASSANDRA_PASSWORD            | -                                                  | 9.3.2.29      | Password to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                    |
| CASSANDRA_CONSISTENCY         | LOCAL_ONE                                          | 9.3.2.25      | Write consistency level as described in [https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html)                                                                                                                                                            |
| CASSANDRA_REPLICATION         | -                                                  | 9.3.2.38      | Allow change replication strategy from SimpleStrategy (use by default, data write without replication) to NetworkReplicationStrategy (data will write with specified replication factor, for DC specified in `CASSANDRA_DC`). Please pay attention that replication factor more that 1 required Cassandra cluster with the same number of nodes. Example: `CASSANDRA_REPLICATION=3` |
| CASSANDRA_CERT                | -                                                  | 9.3.2.25      | Base64-encoded certificate of Cassandra server to be added to java keystore. For example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html)                                                                                                                     |
| CASSANDRA_CONFIG              | -                                                  | 9.3.2.25      | `application.conf` file with custom parameters for Cassandra drivers. For example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html). To connect to AWS please use 4.x DataStax Java driver for Apache Cassandra and the SigV4 authentication plugin            |
| CASSANDRA_REQUEST_TIMEOUT     | 30s                                                | 9.3.2.58      | Timeout of requests to select data from Cassandra                                                                                                                                                                                                                                                                                                                                   |
| CASSANDRA_SCHEMA_INIT_TIMEOUT | 60s                                                | 9.3.2.58      | Timeout to initialize schema in Cassandra                                                                                                                                                                                                                                                                                                                                           |
<!-- markdownlint-enable line-length -->

**Storage: ElasticSearch**

<!-- markdownlint-disable line-length -->
| Parameter       | Default value | Support since | Description                                                                                 |
|-----------------|---------------|---------------|---------------------------------------------------------------------------------------------|
| ES_PROTOCOL     | -             | 9.3.2.58      | Connection protocol of OpenSearch/ElasticSearch. Available values: http, https.             |
| ES_HOST         | -             | 9.3.2.29      | Host of OpenSearch/ElasticSearch storage in cloud. Ignored if `CASSANDRA_HOST` is specified |
| ES_PORT         | -             | 9.3.2.29      | Port of OpenSearch/ElasticSearch storage in cloud. Ignored if `CASSANDRA_HOST` is specified |
| ES_USERNAME     | admin         | 9.3.2.29      | Username to connect to OpenSearch/ElasticSearch                                             |
| ES_PASSWORD     | admin         | 9.3.2.29      | Password to connect to OpenSearch/ElasticSearch                                             |
| ES_SHARDS       | -             | 9.3.2.35      | Number of shards in OpenSearch/ElasticSearch                                                |
| ES_REPLICAS     | -             | 9.3.2.35      | Number of replicas in OpenSearch/ElasticSearch                                              |
| ES_INDEX_PREFIX | -             | 9.3.2.64      | Index prefix value including a delimiter (e.g. 'esc_')                                      |
<!-- markdownlint-enable line-length -->

**Authentication**

> **Note:**
>
> CDT UI reads credentials only once during the container start.
> But AppDeployer or Helm may upgrade only the Secret with credentials if user changes only auth parameters.
> In this case, you must manually restart the CDT UI (pod with name like cdt-ui-service).

**OpenID Connect (OAuth)**

<!-- markdownlint-disable line-length -->

| Parameter                          | Default value | Support since | Description                                                                                                    |
|------------------------------------|---------------|---------------|----------------------------------------------------------------------------------------------------------------|
| ui.security.oidc.idp_url           | -             | -             | Issuer URL of OIDC Identity Provider (e.g. Keycloak).<br/>Please note we need to define `extra_certs` as well. |
| ui.security.oidc.idp_client_id     | -             | -             | Client ID of the profiler on Identity Provider.                                                                |
| ui.security.oidc.idp_client_secret | -             | -             | Client Secret of the profiler on Identity Provider                                                             |

<!-- markdownlint-enable line-length -->

    ```yaml
    ui:
    security:
      oidc:
        idp_url: <Keycloak env relam url>
        idp_client_id: <client id>
        idp_client_secret: <client secret>
    ```

> Note: For Identity Provider configuration details, please refer [keyclock doc](keycloak.md)

**HTTP Basic**

<!-- markdownlint-disable line-length -->

| Parameter                               | Default value  | Support since | Description                                                                                                                                                                                                    |
|-----------------------------------------|----------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ui.security.basic.username              | -              | -             | Secures access to the profiler and limits access only for the user with the given username.Added ui.security.basic.username parameter will enable Quarkus Basic Authentication, otherwise it will be disabled. |
| ui.security.basic.password              | -              | -             | Password for the username described above                                                                                                                                                                      |
| ui.security.basic.credentialsSecretName | ui-credentials | -             | (Optional) Name of the credential secret which will be stored in `secret`                                                                                                                                      |

<!-- markdownlint-enable line-length -->

    ```yaml
    ui:
    security:
      basic:
        credentialsSecretName: <string>
        username: <string>
        password: <string>
    ```

**Extra Cert**  

The `extra_certs` parameter allows specifying a list of certificates which should to include into JVM keystore.
Certificates should be specified in format as mentioned [here](examples/keycloak/extra_certs.yaml). They are stored in
Kuberentes config map `ui-extra-certificates`

**Diagnostic Info Manager**

<!-- markdownlint-disable line-length -->
| Parameter                            | Default value | Support since | Description                                                                                                                                                 |
|--------------------------------------|---------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DIM_BACK_OFF_PERIOD_MILLIS           | 30000         |               | Time interval between retries                                                                                                                               |
| DIM_CANCEL_TIMEOUT_MILLIS            | 30000         |               | Global timeout for cancel collectors after reach timeout                                                                                                    |
| DIM_EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS | 45000         |               | Time for job’s graceful canceling                                                                                                                           |
| DIM_RETENTION_PERIOD                 | 2             |               | Allow to specify retention period which determine how long DIM data will store. Should use with "DIM_RETENTION_DIMENSION". For example: `2`                 |
| DIM_RETENTION_DIMENSION              | d             |               | Allow to specify dimension for retention period. Supported values: s or S - seconds, m - minutes, h or H - hours, d or D - days, W or w - weeks, M - months |
| DIM_RETENTION_SCHEDULE               | `0 0 0 * * ?` |               | Allow to specify when need to run cleanup data in Cron like syntax. For example: `0 0 0 * * ?`                                                              |
| DIM_WORKING_DIRECTORY                | ""            |               | Path to the directory, where DIM will create and delete (according to the retention policy) temp files and job results.                                     |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: ESC exporter**

<!-- markdownlint-disable line-length -->
| Parameter                | Default value | Support since | Description                                                                                                                              |
|--------------------------|---------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------|
| DIM_ESC_EXPORTER_ENABLED | false         |               | Enable "Executor Statistic Collector" (ESC) collector and allow to collect diagnostic information from ESC for next issues investigation |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Grafana exporter**

<!-- markdownlint-disable line-length -->
| Parameter                                    | Default value | Support since | Description                                                                                                                                    |
|----------------------------------------------|---------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| DIM_GRAFANA_ENABLED                          | false         |               | Enable Grafana collector and allow to collect dashboards from Grafana for next issues investigation                                            |
| DIM_GRAFANA_EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS | 300000        |               | Allow to specify timeout of export. After reach this timeout operation will cancel.                                                            |
| DIM_GRAFANA_URL                              | -             |               | Grafana URL to fetch full list of dashboards, download and store them in archive with exported data. For example: `https://grafana.cloud.name` |
| DIM_GRAFANA_USER                             | -             |               | Grafana user to fetch dashboard. For example: `admin`                                                                                          |
| DIM_GRAFANA_PASSWORD                         | -             |               | Grafana password to fetch dashboard. For example: `admin`                                                                                      |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Graylog exporter**

<!-- markdownlint-disable line-length -->
| Parameter                                    | Default value | Support since | Description                                                                                                                                            |
|----------------------------------------------|---------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| DIM_GRAYLOG_ENABLED                          | false         |               | Enable Graylog collector and allow to collect logs for specified period from Graylog for next issues investigation                                     |
| DIM_GRAYLOG_URL                              | -             |               | Graylog URL to fetch logs and include them into diagnostic archive. For example: `https://graylog.vm.org`                                              |
| DIM_GRAYLOG_DEFAULT_STREAM                   | -             |               | Stream ID which CDT will use to export data from Graylog. Usually make sense to specify "All Messages" stream. For example: `000000000000000000000001` |
| DIM_GRAYLOG_DEFAULT_QUERY                    | -             |               | Query (with using Graylog Query Language) which will apply for select logs and add them into diagnostic archive. For example: `namespace: cloudbss`    |
| DIM_GRAYLOG_USER                             | -             |               | Graylog user to fetch logs. For example: `admin`                                                                                                       |
| DIM_GRAYLOG_PASSWORD                         | -             |               | Graylog password to fetch logs. For example: `admin`                                                                                                   |
| DIM_GRAYLOG_TOKEN                            | -             |               | Graylog token which can be used instead of basic credentials (user and password) to fetch logs from Graylog. For example: `xxxx-xxxx-xxxx-xxxx`        |
| DIM_GRAYLOG_EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS | 30000         |               | Allow to specify timeout of export. After reach this timeout operation will cancel.                                                                    |
| DIM_GRAYLOG_THRESHOLD_MESSAGES_FOR_PART      | 100000        |               | Allow to specify limit of exported logs.                                                                                                               |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: InfluxDB exporter**

<!-- markdownlint-disable line-length -->
| Parameter                                   | Default value | Support since | Description                                                                                                             |
|---------------------------------------------|---------------|---------------|-------------------------------------------------------------------------------------------------------------------------|
| DIM_INFLUX_ENABLED                          | false         |               | Enable InfluxDB collector and allow to collect metrics from InfluxDB for specified period for next issues investigation |
| DIM_INFLUX_URL                              | -             |               | InfluxDB's URL to fetch metrics which store in InfluxDB. For example: `https://monitoring.vm.org`                       |
| DIM_INFLUX_USER                             | -             |               | InfluxDB's user to fetch metrics. Often InfluxDB has no enabled authentication. For example: `admin`                    |
| DIM_INFLUX_PASSWORD                         | -             |               | InfluxDB's password to fetch metrics. Often InfluxDB has no enabled authentication. For example: `admin`                |
| DIM_INFLUX_DEFAULT_DATABASE                 | -             |               | Database name which will use to select and export metrics. For example: `cloud_dns_name`                                |
| DIM_INFLUX_EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS | 30000         |               | Allow to specify timeout of export. After reach this timeout operation will cancel.                                     |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Microservice examiner**

<!-- markdownlint-disable line-length -->
| Parameter                                                  | Default value | Support since | Description                                                                         |
|------------------------------------------------------------|---------------|---------------|-------------------------------------------------------------------------------------|
| DIM_MICROSERVICE_EXAMINER_ENABLED                          | false         |               | Enable microservice examiner                                                        |
| DIM_MICROSERVICE_EXAMINER_EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS | 30000         |               | Allow to specify timeout of export. After reach this timeout operation will cancel. |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Prometheus exporter**

<!-- markdownlint-disable line-length -->
| Parameter              | Default value | Support since | Description                                                                                                                 |
|------------------------|---------------|---------------|-----------------------------------------------------------------------------------------------------------------------------|
| DIM_PROMETHEUS_ENABLED | false         |               | Enable Prometheus collector and allow to collect metrics from Prometheus for specified period for next issues investigation |
| DIM_PROMETHEUS_URL     | -             |               | URL to Prometheus API which will use for collect metrics                                                                    |
| DIM_PROMETHEUS_QUERY   | -             |               | Query which will use for collect metrics from Prometheus                                                                    |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Jaeger exporter**

<!-- markdownlint-disable line-length -->
| Parameter          | Default value | Support since | Description                                                                                            |
|--------------------|---------------|---------------|--------------------------------------------------------------------------------------------------------|
| DIM_JAEGER_ENABLED | false         |               | Enable Jaeger collector and allow to collect traces for specified period for next issues investigation |
| DIM_JAEGER_URL     | -             |               | URL to Jaeger Query API which will use to collect traces                                               |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: System Info exporter**

<!-- markdownlint-disable line-length -->
| Parameter                         | Default value | Support since | Description |
|-----------------------------------|---------------|---------------|-------------|
| DIM_SYSTEM_INFO_COLLECTOR_ENABLED | false         |               |             |
| PROJECT_NAME_LIST                 | -             |               |             |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Global HTTP settings**

<!-- markdownlint-disable line-length -->
| Parameter                                       | Default value | Support since | Description                                                               |
|-------------------------------------------------|---------------|---------------|---------------------------------------------------------------------------|
| MAX_CONNECTIONS_PER_ROUTE                       | 20            |               | Allow to specify max http connections per route                           |
| MAX_CONNECTION_IDLE_TIME_MILLIS                 | 60000         |               | Allow to specify idle timeout to http connections                         |
| MAX_TOTAL_CONNECTIONS                           | 20            |               | Allow to specify max pool http connection size                            |
| DIM_MAX_RETRY_COUNT                             | 10            |               | Allow to specify how much retries DIM will execute per exporter/collector |
| REST_TEMPLATE_CONNECTION_REQUEST_TIMEOUT_MILLIS | 80000         |               | Allow to specify timeout for each request                                 |
| REST_TEMPLATE_CONNECT_TIMEOUT_MILLIS            | 80000         |               | Allow to specify timeout for each connection                              |
| REST_TEMPLATE_READ_TIMEOUT_MILLIS               | 120000        |               | Allow to specify timeout for each read request                            |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Global threshold settings**

<!-- markdownlint-disable line-length -->
| Parameter                                 | Default value | Support since | Description                                                                                                                                                                                                   |
|-------------------------------------------|---------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| THRESHOLD_DIM_MAX_ALLOCATED_SPACE         | 20            |               | Allow to specify max space which DIM can use for store diagnostic information in Gb.                                                                                                                          |
| THRESHOLD_TOTAL_MAX_USED_SPACE_PERCENTAGE | 60            |               | Allow to specify how much percent of total available space can be used for store diagnostic information. For example if total available 10 Gb, so we can write maximum 6 Gb after which will raise threshold. |
<!-- markdownlint-enable line-length -->

**Diagnostic Info Manager: Global task settings**

<!-- markdownlint-disable line-length -->
| Parameter                         | Default value | Support since | Description |
|-----------------------------------|---------------|---------------|-------------|
| POD_CANCEL_POLLING_TIMEOUT_MILLIS | 30000         |               |             |
| POD_POLLING_DELAY_MILLIS          | 10000         |               |             |
| POD_POLLING_TIMEOUT_MILLIS        | 1800000       |               |             |
<!-- markdownlint-enable line-length -->

**Agent**

<!-- markdownlint-disable line-length -->
| Parameter                   | Default value       | Support since | Description                                                                                                                                                 |
|-----------------------------|---------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NC_DIAGNOSTIC_MODE          | prod                | 9.3.2.25      | Allow to enable self-profiling of collector-service. Parameter have the same values as in Supporting  profiling and tracing in JAVA service                 |
| NC_DIAGNOSTIC_AGENT_SERVICE | nc-diagnostic-agent | 9.3.2.25      | Allow to override host where agent will send diagnostic information. Parameter have the same values as in Supporting  profiling and tracing in JAVA service |
| ZOOKEEPER_ENABLED           |                     |               | Allow to enable ZooKeeper integration. ESC Agent can fetch settings from ZooKeeper and even a custom config for agent.                                      |
| ZOOKEEPER_ADDRESS           |                     |               | Address of ZooKeeper for fetch custom agent's settings.                                                                                                     |
<!-- markdownlint-enable line-length -->

Also, ui-service support service-level deployment parameters which can be specified only by using deploy profile:

<!-- markdownlint-disable line-length -->
| Parameter      | Default value                                                                                                                                                  | Support since | Description                                                                                                                                         |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| REPLICAS       | 1                                                                                                                                                              | -             | Number of expected replicas of ui-service                                                                                                           |
| JAVA_OPTIONS   | -Xmx256m -Xms256m -XX:MaxDirectMemorySize=64M -Dhttp.maxConnections=32 -Xss512k -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m -XX:ReservedCodeCacheSize=64M | -             | Allow to specify list of Java arguments which will apply to JVM. For example: /usr/bin/java ${JAVA_OPTIONS} -Djava.security.egd=file:/dev/./urandom |
| CPU_REQUEST    | 100m                                                                                                                                                           | -             | Allow to set CPU request for pod                                                                                                                    |
| MEMORY_REQUEST | 650Mi                                                                                                                                                          | -             | Allow to set Memory request for pod                                                                                                                 |
| CPU_LIMIT      | 4                                                                                                                                                              | -             | Allow to set CPU limits for pod                                                                                                                     |
| MEMORY_LIMIT   | 650Mi                                                                                                                                                          | -             | Allow to set Memory limits for pod                                                                                                                  |
<!-- markdownlint-enable line-length -->

## Static service

Static-service is a service which should receive diagnostic information such as GC logs, various dumps and store it on
Persistence Volume.

Maybe we will remove it in the future. But currently it's a mandatory component.

The following configuration options are available:

<!-- markdownlint-disable line-length -->
| Parameter                   | Default value                                                                    | Support since | Description                                                                                                                                                                                                                                                                                                                                                                                                        |
|-----------------------------|----------------------------------------------------------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| STATIC_HOST                 | {{ .Values.SERVICE_NAME }}-{{ .Values.NAMESPACE }}.{{ .Values.SERVER_HOSTNAME }} | -             | Allow to override Ingress host address for static-service. By default host will generate with using: service-name + namespace + cloud dns name                                                                                                                                                                                                                                                                     |
| DIAG_PV_NAME                | -                                                                                |               | When specified and DIAG_HTTP_STORAGE_HOST is not specified, the static service will save /diagnostic put requests to that PV. Make sure that PV is created with ReadWriteMany access mode and that it is writable for a user that cdt-static-service will be running with (10001 by default). The specified PV will also be mounted to diagnostic info manager service to extract data from via the Big Red Button |
| DIAG_PV_MOUNT_PATH          | -                                                                                |               | Allow to specify place in which will store diagnostic information in case when static-service will store file instead of proxy them to collector-service. DIAG_PV_NAME and DIAG_PV_MOUNT_PATH should be specified together to correct store diagnostic info into static-service.                                                                                                                                   |
| DIAG_PV_SIZE                | 1Gi                                                                              | 9.3.2.25      | Size to put in PVC                                                                                                                                                                                                                                                                                                                                                                                                 |
| DIAG_PV_STORAGE_CLASS       | empty string                                                                     | 9.3.2.25      | Can be used with or without DIAG_PV_NAME if auto provisioning is required                                                                                                                                                                                                                                                                                                                                          |
| DIAG_PV_EMPTYDIR            | false                                                                            | 9.3.2.29      | If set to "true", diagnostic volume will be created as an EmptyDir volume under cdt-static-service. For dev and testing purposes only. not recommended for production                                                                                                                                                                                                                                              |
| DIAG_PV_HOURS_ARCHIVE_AFTER | 2                                                                                | -             | in case DIAG_PV_NAME is chosen, after how much time should hourly ZIP archives be created from the collected data to save space. Fresh data may need to be unzipped for convenience                                                                                                                                                                                                                                |
| DIAG_PV_DAYS_DELETE_AFTER   | 14                                                                               | -             | in case DIAG_PV_NAME is chosen, after how much time data is removed automatically                                                                                                                                                                                                                                                                                                                                  |
| DIAG_HTTP_STORAGE_HOST      | `http://cdt-collector-service:8080`                                              |               | Allow to specify proxy host where static-service will proxy all requests to it. By default proxy to collector-service.                                                                                                                                                                                                                                                                                             |
<!-- markdownlint-enable line-length -->

Also, static-service support service-level deployment parameters which can be specified only with using deploy profile:

<!-- markdownlint-disable line-length -->
| Parameter      | Default value | Support since | Description                                   |
|----------------|---------------|---------------|-----------------------------------------------|
| REPLICAS       | 1             | -             | Number of expected replicas of static-service |
| CPU_REQUEST    | 100m          | -             | Allow to set CPU request for pod              |
| MEMORY_REQUEST | 20Mi          | -             | Allow to set Memory request for pod           |
| CPU_LIMIT      | 4             | -             | Allow to set CPU limits for pod               |
| MEMORY_LIMIT   | 20Mi          | -             | Allow to set Memory limits for pod            |
<!-- markdownlint-enable line-length -->

## Integration tests

Integration test is a service which allow to generate some fake calls for ESC and should simplify tests.

<!-- markdownlint-disable line-length -->
| Parameter          | Default value                                                                    | Support since | Description                                                                                                                                   |
|--------------------|----------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| TEST_HOST          | {{ .Values.SERVICE_NAME }}-{{ .Values.NAMESPACE }}.{{ .Values.SERVER_HOSTNAME }} | -             | Allow to override Ingress host address for test-service. By default host will generate with using: servicer-name + namespace + cloud dns name |
| MONITORING_ENABLED | true                                                                             |               | Enable deploy monitoring objects which enable integration with monitoring. In case test-service it is ServiceMonitor.                         |
<!-- markdownlint-enable line-length -->

**Storage: Cassandra**

<!-- markdownlint-disable line-length -->
| Parameter                     | Default value                                      | Support since | Description                                                                                                                                                                                                                                                                                                                                                                     |
|-------------------------------|----------------------------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASSANDRA_HOST                | -                                                  | 9.3.2.29      | Hostname of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                          |
| CASSANDRA_PORT                | 9042                                               | -             | Port of Cassandra storage in cloud                                                                                                                                                                                                                                                                                                                                              |
| CASSANDRA_KEYSPACE            | esc                                                | -             | Name of Cassandra space in cloud                                                                                                                                                                                                                                                                                                                                                |
| CASSANDRA_DC                  | datacenter1 if connecting to localhost, dc1 if not | 9.3.2.25      | Specify Cassandra datacenter. Added default values for compatibility with default docker image and default cloud distribution.                                                                                                                                                                                                                                                  |
| CASSANDRA_USERNAME            | -                                                  | 9.3.2.29      | Username to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                |
| CASSANDRA_PASSWORD            | -                                                  | 9.3.2.29      | Password to connect to Cassandra                                                                                                                                                                                                                                                                                                                                                |
| CASSANDRA_CONSISTENCY         | LOCAL_ONE                                          | 9.3.2.25      | Write consistency level as described in [https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html)                                                                                                                                                        |
| CASSANDRA_REPLICATION         | -                                                  | 9.3.2.38      | Allow change replication strategy from SimpleStrategy (use by default, data write without replication) to NetworkReplicationStrategy (data will write with specified replication factor, for DC specified in CASSANDRA_DC). Please pay attention that replication factor more that 1 required Cassandra cluster with the same number of nodes. Example: CASSANDRA_REPLICATION=3 |
| CASSANDRA_CERT                | -                                                  | 9.3.2.25      | Base64-encoded certificate of Cassandra server to be added to java keystore. for example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html)                                                                                                                 |
| CASSANDRA_CONFIG              | -                                                  | 9.3.2.25      | application.conf file with custom parameters for Cassandra drivers. for example, this one [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html) to connect to AWS please use 4.x DataStax Java driver for Apache Cassandra and the SigV4 authentication plugin           |
| CASSANDRA_REQUEST_TIMEOUT     | 30s                                                | 9.3.2.58      | Timeout of requests to select data from Cassandra                                                                                                                                                                                                                                                                                                                               |
| CASSANDRA_SCHEMA_INIT_TIMEOUT | 60s                                                | 9.3.2.58      | Timeout to initialize schema in Cassandra                                                                                                                                                                                                                                                                                                                                       |
<!-- markdownlint-enable line-length -->

**Test service specific settings**

<!-- markdownlint-disable line-length -->
| Parameter             | Default value                       | Support since | Description                                    |
|-----------------------|-------------------------------------|---------------|------------------------------------------------|
| ESC_COLLECTOR_SERVICE | `http://cdt-collector-service:8080` |               | Allow to specify link to ESC collector service |
| ESC_UI_SERVICE        | `http://cdt-ui-service:8180`        |               | Allow to specify link to ESC ui service        |
| ESC_STATIC_SERVICE    | `http://cdt-static-service:8080`    |               | Allow to specify link to ESC static service    |
<!-- markdownlint-enable line-length -->

**Agent**

<!-- markdownlint-disable line-length -->
| Parameter                   | Default value         | Support since | Description                                                                                                                                                 |
|-----------------------------|-----------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NC_DIAGNOSTIC_MODE          | prod                  | 9.3.2.25      | Allow to enable self-profiling of collector-service. Parameter have the same values as in Supporting  profiling and tracing in JAVA service                 |
| NC_DIAGNOSTIC_AGENT_SERVICE | cdt-collector-service | 9.3.2.25      | Allow to override host where agent will send diagnostic information. Parameter have the same values as in Supporting  profiling and tracing in JAVA service |
| ZOOKEEPER_ENABLED           |                       |               | Allow to enable ZooKeeper integration. ESC Agent can fetch settings from ZooKeeper and even a custom config for agent.                                      |
| ZOOKEEPER_ADDRESS           |                       |               | Address of ZooKeeper for fetch custom agent's settings.                                                                                                     |
<!-- markdownlint-enable line-length -->

Also, test-service support service-level deployment parameters which can be specified only with using deploy profile:

<!-- markdownlint-disable line-length -->
| Parameter      | Default value                                                                                                                                                    | Support since | Description                                                                                                                                         |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| REPLICAS       | 1                                                                                                                                                                | -             | Number of expected replicas of test-service                                                                                                         |
| JAVA_OPTIONS   | `-Xmx512m -Xms256m -XX:MaxDirectMemorySize=64M -Dhttp.maxConnections=32 -Xss512k -XX:MetaspaceSize=150m -XX:MaxMetaspaceSize=150m -XX:ReservedCodeCacheSize=64M` | -             | Allow to specify list of Java arguments which will apply to JVM. For example: /usr/bin/java ${JAVA_OPTIONS} -Djava.security.egd=file:/dev/./urandom |
| CPU_REQUEST    | 100m                                                                                                                                                             | -             | Allow to set CPU request for pod                                                                                                                    |
| MEMORY_REQUEST | 20Mi                                                                                                                                                             | -             | Allow to set Memory request for pod                                                                                                                 |
| CPU_LIMIT      | 4                                                                                                                                                                | -             | Allow to set CPU limits for pod                                                                                                                     |
| MEMORY_LIMIT   | 20Mi                                                                                                                                                             | -             | Allow to set Memory limits for pod                                                                                                                  |
<!-- markdownlint-enable line-length -->

## Pprof collector

The service for scraping profiling information in [pprof](https://github.com/google/pprof) format from Kubernetes pods
in the pull format via HTTP.

If you haven't enabled pprof profiling in your application yet, you can see more info about it
[in this guide](./user-guides/enable-pprof-in-your-app.md).

Example of scrape config that should be set to the `PPROF_CONFIG` parameter:
[full scrape config example](./examples/pprof-collector/full-scrape-config.yaml).
A detailed explanation of this configuration can be found in
[the Pprof collector scrape config document](./user-guides/pprof-collector-scrape-config.md).

The following configuration options are available:

<!-- markdownlint-disable line-length -->
| Parameter           | Default value | Support since | Description                                                                                                              |
|---------------------|---------------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| PPROF_INSTALL       | false         | -             | Allow to enable pprof-collector installation.                                                                            |
| PPROF_LOG_LEVEL     | info          | -             | Allows customizing the log level for logger.                                                                             |
| PPROF_PUSH_INTERVAL | 5m            | -             | Interval between pushes to the storage.                                                                                  |
| PPROF_CONFIG        | -             | -             | Configuration of pprof-collector that contains scrape configuration.                                                     |
| MONITORING_ENABLED  | true          | -             | Enable deploy monitoring objects which enable integration with monitoring. In case pprof-collector it is ServiceMonitor. |
<!-- markdownlint-enable line-length -->

**Storage: OpenSearch**

Storage parameters include general parameters available for each service as well as pprof-collector specific params
(have `PPROF_` prefix)

<!-- markdownlint-disable line-length -->
| Parameter                          | Default value | Support since | Description                                                                     |
|------------------------------------|---------------|---------------|---------------------------------------------------------------------------------|
| ES_PROTOCOL                        | http          | -             | Connection protocol of OpenSearch/ElasticSearch. Available values: http, https. |
| ES_HOST                            | -             | -             | Host of OpenSearch/ElasticSearch storage in cloud                               |
| ES_PORT                            | 9200          | -             | Port of OpenSearch/ElasticSearch storage in cloud                               |
| ES_USERNAME                        | admin         | -             | Username to connect to OpenSearch/ElasticSearch                                 |
| ES_PASSWORD                        | admin         | -             | Password to connect to OpenSearch/ElasticSearch                                 |
| ES_SHARDS                          | 5             | -             | Number of shards in OpenSearch/ElasticSearch                                    |
| ES_REPLICAS                        | 1             | -             | Number of replicas in OpenSearch/ElasticSearch                                  |
| ES_INDEX_PREFIX                    | -             | -             | Index prefix value including a delimiter (e.g. 'cdt-')                          |
| PPROF_ES_DELETE_INDICES_AFTER_DAYS | -             | -             | Indices created more than N days ago will be deleted                            |
| PPROF_ES_TLS_ENABLED               | -             | -             | Allows to enable TLS for OpenSearch                                             |
| PPROF_ES_TLS_SKIP_VERIFY           | -             | -             | Allows to enable skip verify for self-signed certificates                       |
<!-- markdownlint-enable line-length -->

**Cluster-wide resources**

The pprof-collector needs `ClusterRole` and `ClusterRoleBinding` for correct work, but in some cases these cluster-wide
resources cannot be created during the deployment. You can learn more about it from
[the RBAC documentation](./rbac/rbac.md).

You can enable or disable creation of the cluster-wide resources via the following parameter:

<!-- markdownlint-disable line-length -->
| Parameter         | Default value | Support since | Description                                                                                                                                                                       |
|-------------------|---------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PRIVILEGED_RIGHTS | true          | -             | If set to `true` deploy all resources. Otherwise, deploy only Role and RoleBinding resources and expect that ClusterRole and ClusterRoleBinding resources were deployed manually. |
<!-- markdownlint-enable line-length -->

Also, pprof-collector supports service-level deployment parameters which can be specified only with using deploy profile:

<!-- markdownlint-disable line-length -->
| Parameter      | Default value | Support since | Description                                    |
|----------------|---------------|---------------|------------------------------------------------|
| REPLICAS       | 1             | -             | Number of expected replicas of pprof-collector |
| CPU_REQUEST    | 50m           | -             | Allow to set CPU request for pod               |
| MEMORY_REQUEST | 128Mi         | -             | Allow to set Memory request for pod            |
| CPU_LIMIT      | 100m          | -             | Allow to set CPU limits for pod                |
| MEMORY_LIMIT   | 512Mi         | -             | Allow to set Memory limits for pod             |
<!-- markdownlint-enable line-length -->

# Smoke test

Firstly, check deploy and pod statuses of storage which use for ESC. For example, you can check at least
that all pods are running and didn't restart.

    ```bash
    kubectl get pods -n <namespace>
    ```

> **Note:**
>
> In case, when storage will use any managed service like AWS Keyspace,
> you need to check that this service is activated in your account and is working now.

Second, you can check that all components are up using the following command:

    ```bash
    kubectl get pods -n <esc_namespace>
    ```

For example, the typical ESC deployments container the following pods:

    ```text
    ❯ kubectl get pods -n profiler
    NAME                                     READY   STATUS    RESTARTS   AGE
    cdt-collector-service-84bbf5cc88-tpmnw   1/1     Running   0          6m6s
    cdt-static-service-65c9fbf96b-n6nqz      1/1     Running   0          5m53s
    cdt-ui-service-779897f59-xrwmg           1/1     Running   0          4m53s
    ```

Or you can use the Kubernetes Dashboard to see pods and their status in the UI.

Thirdly, if you want to use `nc-diagnostic-agent` to send traces from microservices to ESC you need
to check that it was deployed in the namespace with an application.

You can use the command:

    ```bash
    kubectl get pods -n <namespace> --selector=app.kubernetes.io/name=nc-diagnostic-agent
    ```

> **Note:**
>
> Please pay attention that `nc-diagnostic-agent` deployment and pod should be not in the namespace with ESC.
> It should deploy in the namespace with an application.

If it is presented, you need to check that in environment variables it contains the variable:

    ```yaml
    - name: ESC_COLLECTOR_NS
      value: <cdt-namespace-name>
    ```

To print a list of environment variables, you can use the command:

    ```bash
    kubectl get pods -n <namespace> --selector=app.kubernetes.io/name=nc-diagnostic-agent -o yaml
    ```

Or you can use the Kubernetes Dashboard to see check `nc-diagnostic-agent` and its settings.

Fourth, you can check that ESC collected the calls. You need open ESC UI (see a list of Ingresses for it) and:

1. Click on the field to select namespace/svc/pods and select namespace with Application
2. In the tree, select any pod and click download buttons for GC logs, Top dumps, Thread dump
3. Check that will download archives with nonempty content (should contain at least one file)

For example, you should see something like in the following screenshot:

![Call Tree](/docs/images/user_guide/calls-list/calls_list.png)
