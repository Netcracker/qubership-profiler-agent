# Settings

## Table of Content

<!-- TOC -->
* [Settings](#settings)
  * [Table of Content](#table-of-content)
  * [Environment variables](#environment-variables)
    * [Test Run options](#test-run-options)
    * [Test Data](#test-data)
    * [Prometheus Remote Write settings](#prometheus-remote-write-settings)
  * [TestOptions](#testoptions)
<!-- TOC -->

## Environment variables

### Test Run options

| Name                      | Example            | Description                      |
|---------------------------|--------------------|----------------------------------|
| `COLLECTOR_HOST`          | `localhost`        | collector service (without port) |
| `LOG_LEVEL`               | `info`             | log level in emulator            |
| `DURATION`                | `10m`              | max duration                     |
| `PODS`                    | `10`               |                                  |

> Should use change `LOG_LEVEL=debug` **ONLY** if emulating small count of services due to amount of log data

### Test Data

| Name                      | Example                 | Description           |
|---------------------------|-------------------------|-----------------------|
| `EMULATOR_NAMESPACE`      | `test_namespace`        |                       |
| `EMULATOR_SERVICE_PREFIX` | `test_service`          |                       |
| `EMULATOR_POD_PREFIX`     | `test-service-85asd-wa` |                       |

Each emulated pod will get uniq number in range `1...PODS`, which will be added to service and pod prefixes.

### Prometheus Remote Write settings

| Name                           | Example                                                | Description                                                                                                                                              |
|--------------------------------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| K6_PROMETHEUS_RW_SERVER_URL    | `http://vmsingle-k8s.monitoring.svc:8429/api/v1/write` | URL of the Prometheus remote write implementation’s endpoint.                                                                                            |
| K6_PROMETHEUS_RW_USERNAME      | `admin`                                                | Username for the HTTP Basic authentication at the Prometheus remote write endpoint.                                                                      |
| K6_PROMETHEUS_RW_PASSWORD      | `admin`                                                | Password for the HTTP Basic authentication at the Prometheus remote write endpoint.                                                                      |
| K6_PROMETHEUS_RW_PUSH_INTERVAL | `10s`                                                  | Interval between the metrics’ aggregation and upload to the endpoint.                                                                                    |
| K6_PROMETHEUS_RW_TREND_STATS   | `min,max,avg,p(90),p(95)`                              | Defines the stats functions to map for all of the defined trend metrics. It’s a comma-separated list of stats functions to include (e.g. p(90),avg,sum). |

More options for Prometheus Remote Write can be found [on this page](https://grafana.com/docs/k6/latest/results-output/real-time/prometheus-remote-write/#options).

## TestOptions

Can be overridden in `common.js` file, but after that image should be rebuilt.

| Name               | Example | Description                                                                             |
|--------------------|---------|-----------------------------------------------------------------------------------------|
| `host`             | -       |                                                                                         |
| `log`              | -       |                                                                                         |
| `timeout.connect`  | `1s`    | timeout for initial connect to collector                                                |
| `timeout.session`  | `120s`  | timeout for tcp session of emulated in `scenario.tcp.js`<br/> (see `tcp_communication`) |
| `duration`         | -       |                                                                                         |
| `pods`             | -       |                                                                                         |
| `prefix.namespace` | -       |                                                                                         |
| `prefix.service`   | -       |                                                                                         |
| `prefix.podName`   | -       |                                                                                         |
