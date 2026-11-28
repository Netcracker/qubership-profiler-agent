This guide describes how to add Cloud Diagnostic Toolset Agent and use it in the microservice.

# Table of Content

<!-- TOC -->
* [Table of Content](#table-of-content)
* [TL;DR](#tldr)
* [Requirements](#requirements)
  * [Supported JDKs](#supported-jdks)
  * [Linux utilities](#linux-utilities)
  * [Using directories](#using-directories)
* [Settings](#settings)
  * [Environment variables which cdt agent use](#environment-variables-which-cdt-agent-use)
  * [Environment variables of cdt agent itself](#environment-variables-of-cdt-agent-itself)
    * [Configuration for agent's logging level](#configuration-for-agents-logging-level)
<!-- TOC -->

# TL;DR

Add the ability to set the following two environment variables in your deployment:

```yaml
kind: Deployment
apiVersion: apps/v1
...
spec:
  template:
    spec:
      containers:
        - name: ...
          env:
            - name: NC_DIAGNOSTIC_MODE
              value: {{ .Values.NC_DIAGNOSTIC_MODE  | quote }}
            - name: NC_DIAGNOSTIC_AGENT_SERVICE
              value: {{ .Values.NC_DIAGNOSTIC_AGENT_SERVICE | quote }}
```

In `values.yaml`:

```yaml
NC_DIAGNOSTIC_MODE: off
NC_DIAGNOSTIC_AGENT_SERVICE: nc-diagnostic-agent
```

> **Warning!**
>
> Make sure to install `nc-diagnostic-agent` in this namespace so it can act as a proxy to CDT.

During deployment, you can enable the CDT agent using the following parameters:

```yaml
NC_DIAGNOSTIC_MODE: prod
```

# Requirements

## Supported JDKs

CDT agent supports `JDK 17` and `JDK 21`

## Linux utilities

The following tools are required for the proper functioning of the bash scripts installed with the agent.
These scripts enable the collection of diagnostic information.

Without these tools, the agent continues to run,
but this may lead to various issues — from the inability to collect any information to complete script failures.

Generic tools:

* `bash`
* `gzip`
* `zip`
* `unzip`
* `curl`
* `flock` (with `-w` option that sets number of seconds to wait for lock)
* `find`
* `top -Hb -p ${java_pid} -d 60 -n 1`
  > NOTE: make sure that `-p` options are supported, on alpine usually need to install `procps`
* `pgrep`
* `hostname`

JDK tools:

* `jstack`
* `jmap`

## Using directories

The CDT agent uses several directories to store diagnostic information.  
Almost all of these directories can be overridden during deployment.

The agent itself (`lib/agent.jar`, `diagtools`, `diag-bootstrap.sh`, `diag-lib.sh`, and the default `config/`) lives in
`NC_DIAGNOSTIC_HOME`, which the bootstrap script defaults to its own folder: `/app/diag` in the cloud core image. Only
the `dev` mode writes there: it unpacks agent updates into that folder, and falls back to `prod` when it cannot. In
other modes the folder can stay on a read-only root filesystem. Do not mount an `emptyDir` over it: the mount hides the
agent.

If `NC_DIAGNOSTIC_FOLDER` is unset and `/app/ncdiag` does not exist or is not writable, the agent writes its runtime
data into `NC_DIAGNOSTIC_HOME`. It does the same when the agent in `NC_DIAGNOSTIC_HOME` predates `NC_DIAGNOSTIC_HOME`,
and after a `dev` mode update.

The directories in the table below require read-write access. If your pod runs with a read-only root filesystem, mount
ephemeral storage (such as `emptyDir`) at each of them.

<!-- markdownlint-disable line-length -->
| Default directory      | Environment name           | Make sense override | Description                                                                                                                                                                                |
|------------------------|----------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /tmp/diagnostic        | NC_DIAGNOSTIC_LOGS_FOLDER  | ✗ No                | Use to store information as from cdt itself (logs from cdt agent's bash scripts) and for store diagnostic information by application (like, GC logs, top command, heap dump, thread dump). |
| /app/ncdiag            | NC_DIAGNOSTIC_FOLDER       | ✗ No                | Runtime data of cdt agent: settings from zookeeper or consul, the copy of the agent configuration, local dumps, and other local information.                                              |
| /diagnostic-pv-storage | DIAGNOSTIC_PV_DUMPS_FOLDER | ✗ No                | Use as storage before send diagnostic information to collector-service. Also can be use with Persistence Volume to store diagnostic information between pod runs.                          |
<!-- markdownlint-enable line-length -->

Examples of the information and files that can be stored in these directories:

* `NC_DIAGNOSTIC_LOGS_FOLDER` = `/tmp/diagnostic`

    ```bash
    /tmp/diagnostic/log/                           # bash scripts logs
    /tmp/diagnostic/{heap_dump_name}               # heap dump which JVM will generate after Out Of Memory (OOM) exception
    /tmp/diagnostic/gclog/                         # gc logs which JVM will generate during work
    /tmp/diagnostic/{current_timestamp}.hprof.zip  # zipped heap dump which will send to collector-service
    /tmp/diagnostic/{current_timestamp}.td.txt     # thread dump output which can be generate by bash scripts by scheduler or by request
    /tmp/diagnostic/{current_timestamp}.top.txt    # top output which can be generate by bash scripts by scheduler or by request
    ```

* `NC_DIAGNOSTIC_FOLDER` = `/app/ncdiag`

    ```bash
    /app/ncdiag/config/                                 # copy of the agent configuration, rebuilt on every start
    /app/ncdiag/localdump                               # cdt agent calls dump
    /app/ncdiag/pod.name                                # pod name which cdt agent can read instead of hostname
    /app/ncdiag/start_time.txt                          # contains a start time of pod
    /app/ncdiag/.diagnostic.exclusivelock               # use for generate lock files to avoid some calls for top or thread dumps
    /app/ncdiag/zkproperties/{zookeeper_property_name}  # store name and value from zookeeper to apply in local cdt agent
    /app/ncdiag/zkproperties/cdt.config                 # special case of zookeeper property name, can contains the custom cdt agent config
    /app/ncdiag/config/default/zookeeper.xml            # custom config from /app/ncdiag/zkproperties/cdt.config will move in this file
    ```

* `DIAGNOSTIC_PV_DUMPS_FOLDER` = `/diagnostic-pv-storage`

    ```bash
    /diagnostic-pv-storage/{namespace}/{pod_name}/{container_id}/{modifier}_{HOSTNAME}_{dump_name}  # Which such mask will store all dumps, like top, thread, heap
    /diagnostic-pv-storage/{namespace}/{pod_name}/{container_id}/gclog/                             # In this directory will store all collected gc logs
    ```

# Settings

## Environment variables which cdt agent use

In order for the CDT to identify the microservice, the following variables need to be set:

| Name              | Description              |
|-------------------|--------------------------|
| MICROSERVICE_NAME | Name of the microservice |
| CLOUD_NAMESPACE   | Name of the namespace    |

Pod name will be found automatically using the `hostname` command.
If the variable `CLOUD_NAMESPACE` is not specified,
the CDT agent and bash scripts will use the namespace name from the file `/run/secrets/kubernetes.io/serviceaccount/namespace`,
which is mounted with the Service Account.

The variable `MICROSERVICE_NAME` will be used as the service name (as a human-readable microservice name).

For example, for variables with values:

* `MICROSERVICE_NAME` = test-service
* `CLOUD_NAMESPACE` = cdt

calls will be stored by coordinates:

```bash
CLOUD_NAMESPACE = cdt
MICROSERVICE_NAME = test-service
POD_NAME = test-service-585858d5c-6ttnm
```

## Environment variables of cdt agent itself

The CDT agent and its bash scripts allow specifying some settings using environment variables.
Some of these variables don't make much sense to override, but it's still good to know about such capabilities.

Environment variables which allow to configure cdt agent behavior:

<!-- markdownlint-disable line-length -->
| Name                   | Default | Description                                                                                                                 |
|------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------|
| REMOTE_DUMP_HOST       | -       | Allow to specify host in which cdt agent will push collected calls.                                                         |
| REMOTE_DUMP_PORT       | 1715    | Deprecated! Use REMOTE_DUMP_PORT_PLAIN instead.                                                                             |
| REMOTE_DUMP_PORT_PLAIN | 1715    | Allow to specify port which cdt agent will use to pus collected calls.                                                      |
| REMOTE_DUMP_PORT_SSL   | -       | Allow to specify SSL port which cdt agent will use instead of plain port to send collected calls with using SSL encryption. |
| FORCE_LOCAL_DUMP       | false   |                                                                                                                             |
| CLOUD_NAMESPACE        | -       |                                                                                                                             |
| MICROSERVICE_NAME      | -       |                                                                                                                             |
| LOG_LEVEL              | warn    | Allow to specify log level for the agent logs.                                                                              |
<!-- markdownlint-enable line-length -->

Environment variables that allow you to configure the behavior of bash scripts:

<!-- markdownlint-disable line-length -->
| Name                             | Default             | Mandatory | Description                                                                                                                        |
|----------------------------------|---------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------|
| NC_DIAGNOSTIC_AGENT_SERVICE      | nc-diagnostic-agent | yes       | Allow to specify host to which will send data                                                                                      |
| NC_DIAGNOSTIC_MODE               | off                 | yes       |                                                                                                                                    |
| NC_DIAGNOSTIC_CDT_ENABLED        | true                | no        | Allow to disable attach cdt-agent to process                                                                                       |
| NC_DIAGNOSTIC_CDT_BUFFERS        | ?                   | no        |                                                                                                                                    |
| NC_DIAGNOSTIC_THREADDUMP_ENABLED | true                | no        | Allow to disable thread dump collection by scheduler in pod                                                                        |
| NC_DIAGNOSTIC_TOP_ENABLED        | true                | no        | Allow to disable top collection by scheduler in pod                                                                                |
| NC_DIAGNOSTIC_GC_ENABLED         | true                | no        | Allow to to disable collect GC logs                                                                                                |
| NC_DIAGNOSTIC_DUMPS_ENABLED      | true                | no        | Allow to disable add parameters to enable Heap Dump collection after OOM to JVM Args                                               |
| NC_DIAGNOSTIC_LOGS_FOLDER        | /tmp/diagnostic     | no        | Allow to specify directory which will use to save                                                                                  |
| CDT_LOG_LEVEL                    | `WARN`              | no        | Allow to specify log level for cdt-agent (`DEBUG`, `INFO`, `WARN` (default), `ERROR`, `OFF`) and for bash scripts (no log / debug) |
| DIAGNOSTIC_DUMP_INTERVAL         | 60                  | no        | Allow to specify interval which will use to make thread dump and collect TOP                                                       |
| DIAGNOSTIC_UPLOAD_MAX_AGE        | 48h                 | no        | Pending heap dump / core / hs_err files (`*.hprof*` in `NC_DIAGNOSTIC_LOGS_FOLDER`) older than this are deleted on each scan tick. Accepts Go duration syntax (e.g. `48h`, `30m`). |
| DIAGNOSTIC_PENDING_MAX_BYTES     | 10Gi                | no        | Safety net against disk buildup: if combined size of pending `*.hprof*` exceeds this, oldest files are deleted (by mtime) until total is at or below the limit, always keeping at least the newest file. Accepts K/M/G/T (×1000) or Ki/Mi/Gi/Ti (×1024) suffixes; bare integer = bytes. |
<!-- markdownlint-enable line-length -->

Environment variables that can be used to configure the CDT agent when the cloud core image is not
used and custom bash scripts are provided:

<!-- markdownlint-disable line-length -->
| Name                            | Default                | Description                                                                                                                                                                                                                                         |
|---------------------------------|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NC_DIAGNOSTIC_HOME              | folder of `diag-bootstrap.sh` | Folder where the diagnostic agent is installed: `lib/agent.jar`, `diagtools`, `diag-lib.sh`, and `config/`. Only the `dev` mode writes there, to unpack agent updates. If the folder of `diag-bootstrap.sh` does not hold both `diag-lib.sh` and `lib/agent.jar`, the default is `NC_DIAGNOSTIC_FOLDER`, or legacy `PROFILER_FOLDER` |
| NC_DIAGNOSTIC_FOLDER            | /app/ncdiag            | Folder where the diagnostic agent writes at runtime. It must be writable; mount an `emptyDir` there when the root filesystem is read-only. Do not modify it when the application uses the cloud core image as a base image |
| PROFILER_ENABLED                | false                  | Deprecated! Use NC_DIAGNOSTIC_MODE                                                                                                                                                                                                                  |
| REMOTE_DUMP_HOST                | empty                  | Deprecated! Use NC_DIAGNOSTIC_AGENT_SERVICE                                                                                                                                                                                                         |
| REMOTE_STATIC_HOST              | ?                      | Deprecated!                                                                                                                                                                                                                                         |
| DIAGNOSTIC_CENTER_DUMPS_ENABLED | true                   | Allow to disable send collected diagnostic info into diagnostic center / collector-service                                                                                                                                                          |
| PV_DIAGNOSTIC_DUMPS_ENABLED     | false                  |                                                                                                                                                                                                                                                     |
| GC_LOGS_COLLECTION_ENABLED      | true                   | Allow to disable scan of folder with GC logs                                                                                                                                                                                                        |
| DIAGNOSTIC_PV_DUMPS_FOLDER      | /diagnostic-pv-storage |                                                                                                                                                                                                                                                     |
<!-- markdownlint-enable line-length -->

### Configuration for agent's logging level

* The CDT profiler agent uses the `CDT_LOG_LEVEL` environment variable to set the log level for agent logs.
* If `CDT_LOG_LEVEL` is not defined, the agent wil use the `LOG_LEVEL` variable from the application's environment.

Supported values for `CDT_LOG_LEVEL` and `LOG_LEVEL`, along with descriptions of the log levels:

<!-- markdownlint-disable line-length -->
| Env Variable `CDT_LOG_LEVEL` | Env Variable `LOG_LEVEL` | Agent Log Level | Description                                                                            |
|------------------------------|--------------------------|-----------------|----------------------------------------------------------------------------------------|
| -                            | -                        | `WARN`          | User will see logs with following levels: `WARN` and `ERROR`                           |
| `OFF`                        | Not considered if set    | `OFF`           | No logs will be displayed for the loggers                                              |
| `ERROR`                      | Not considered if set    | `ERROR`         | User will only see logs with `ERROR` level                                             |
| `WARN`                       | Not considered if set    | `WARN`          | User will see logs with following levels: `WARN` and `ERROR`                           |
| `INFO`                       | Not considered if set    | `INFO`          | User will see logs with following levels: `INFO`, `WARN` and `ERROR`                   |
| `DEBUG`                      | Not considered if set    | `DEBUG`         | User will see logs with following levels: `DEBUG`, `INFO`, `WARN` and `ERROR`          |
| `TRACE`                      | Not considered if set    | `TRACE`         | User will see logs with following levels: `TRACE`, `DEBUG`, `INFO`, `WARN` and `ERROR` |
| -                            | `OFF`                    | `OFF`           | No logs will be displayed for the loggers                                              |
| -                            | `ERROR`                  | `ERROR`         | User will only see logs with `ERROR` level                                             |
| -                            | `WARN`                   | `WARN`          | User will see logs with following levels: `WARN` and `ERROR`                           |
| -                            | `INFO`                   | `INFO`          | User will see logs with following levels: `INFO`, `WARN` and `ERROR`                   |
| -                            | `DEBUG`                  | `WARN`          | User will see logs with following levels: `DEBUG`, `INFO`, `WARN` and `ERROR`          |
| -                            | `TRACE`                  | `WARN`          | User will see logs with following levels: `TRACE`, `DEBUG`, `INFO`, `WARN` and `ERROR` |
<!-- markdownlint-enable line-length -->
