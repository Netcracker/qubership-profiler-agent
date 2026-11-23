# Qubership Profiler Agent

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Netcracker/qubership-profiler-agent/badge)](https://scorecard.dev/viewer/?uri=github.com/Netcracker/qubership-profiler-agent)

Qubership Profiler is a continuous tracing profiler for Java applications. Its Java agent attaches to the JVM with
`-javaagent`, records method calls, SQL statements, and service calls, and sends them to a collector. The Go backend
stores the data in S3-compatible storage and serves it to a web UI.

## Features

* Traces of slow requests and errors
* Continuous profiling
* SQL capture (queries and binds)
* Service call capture

## Components

* [`apps/agent`](apps/agent): the Java agent, its instrumentation plugins, and the installer distribution
  (Gradle build).
* [`apps/profiler-backend`](apps/profiler-backend): the Go backend, one binary with the `collect`, `query`, and
  `maintain` subcommands.
* [`apps/ui`](apps/ui): the web UI, served by `query` at `/ui`.
* [`apps/dumps-collector`](apps/dumps-collector): a Go service that indexes the thread, top, and heap dumps agents write
  to a shared volume, keeps their metadata in SQLite, and serves the dumps for download.
* [`apps/examples`](apps/examples): sample Quarkus and Spring Boot services that run with the agent.
* [`deploy/charts`](deploy/charts): Helm charts for `profiler-backend` and `dumps-collector`.
* [`libs`](libs): Go packages shared by the backend services.
* [`tools`](tools): a load generator for the backend, a migration cleaner job, and a generator of sample data for the
  UI.

The agent and its plugins are published to Maven Central under the `org.qubership.profiler` group. The
`qubership-profiler-backend`, `qubership-profiler-dumps-collector`, and `qubership-profiler-transfer` images are built
for `linux/amd64` and `linux/arm64`.

## Documentation

* [Add the agent to a microservice](docs/add-cdt-agent.md): environment variables, supported JDKs, required tools
* [User guide](docs/user-guide.md) and [use cases](docs/use-cases.md) for the web UI
* [Deploy the backend with Helm](deploy/charts/profiler-backend/README.md), including a quick start on kind
* [Run the backend locally with Docker Compose](apps/profiler-backend/README.md#quick-start-docker-compose)
* [Backend design documents](docs/design/README.md): write, read, and storage contracts

## Library and framework instrumentation

Application servers and portals:

* [Liferay](apps/agent/plugins/liferay)

Build systems:

* Apache Ant
  * [Ant 1.10.1 and earlier](apps/agent/plugins/ant)
  * [Ant 1.10.2 and later](apps/agent/plugins/ant_1102)

Databases:

* [Couchbase Java SDK](apps/agent/plugins/couchbase)
* DataStax Java driver for Cassandra
  * [DataStax driver 3.x](apps/agent/plugins/cassandra)
  * [DataStax driver 4.x](apps/agent/plugins/cassandra4)
* [Elasticsearch REST client](apps/agent/plugins/elasticsearch)
* [MySQL JDBC](apps/agent/plugins/mysql)
* [OpenSearch REST client](apps/agent/plugins/opensearch)
* [PostgreSQL JDBC](apps/agent/plugins/postgresql)

Distributed tracing:

* [Brave (Zipkin)](apps/agent/plugins/brave)
* [Jaeger](apps/agent/plugins/jaeger)
* [OpenCensus and OpenTelemetry (Ocelot)](apps/agent/plugins/ocelot)

HTTP clients:

* [Apache HttpClient 3.x and 4.x](apps/agent/plugins/apache_httpclient)
* [JDK `HttpURLConnection`](apps/agent/plugins/java_http_client)
* [Spring `RestTemplate`](apps/agent/plugins/springrest)

HTTP servers:

* [Jetty with `javax.servlet`](apps/agent/plugins/jetty_http)
* [Jetty with `jakarta.servlet`](apps/agent/plugins/jetty10_http)
* [Spring WebFlux on Reactor Netty or Tomcat](apps/agent/plugins/http)
* [Tomcat 9 and earlier](apps/agent/plugins/tomcat_http)
* [Tomcat 10 and later](apps/agent/plugins/tomcat10_http)
* [Undertow before 2.3](apps/agent/plugins/undertow_http)
* [Undertow 2.3 and later](apps/agent/plugins/undertow23_http)
* [Vert.x with RESTEasy (Quarkus)](apps/agent/plugins/vertx)

Java frameworks:

* [Apache Felix](apps/agent/plugins/apache_felix)
* [Eclipse Equinox](apps/agent/plugins/equinox)
* [Spring Framework](apps/agent/plugins/spring)

Loggers:

* [Log4j 1.x](apps/agent/plugins/log4j_enhancer)

Messaging:

* [ActiveMQ](apps/agent/plugins/activemq)
* [HornetQ](apps/agent/plugins/hornetq)
* [RabbitMQ and Spring AMQP](apps/agent/plugins/rabbitmq)

Other:

* [Jackson](apps/agent/plugins/jackson)
* [Quartz Scheduler](apps/agent/plugins/quartz)
* [Rhino](apps/agent/plugins/rhino)

## How to build

### Local build

The Gradle build compiles the agent, its plugins, and the diagnostic tools, and runs their tests. It needs:

* JDK 17 or later to run Gradle. The build compiles with JDK 21 and downloads it when no local JDK 21 is found.
* Go, at the version the root `go.mod` names, `make`, and [Zig](https://ziglang.org), which build the diagnostic tools
  in [`apps/agent/diagtools`](apps/agent/diagtools). CI uses Zig 0.15.2. On macOS, the darwin targets also need the
  Xcode command line tools.
* Docker and network access: the installer tests build a base image from a cloned `qubership-core-base-images`
  repository and run it with Testcontainers.

```bash
git clone https://github.com/Netcracker/qubership-profiler-agent.git
cd qubership-profiler-agent
./gradlew build # builds and tests the agent
./gradlew tasks # lists available tasks
```

The Go backend and the tools build with Go alone. From the repository root, `go test ./...` runs the tests of `apps`,
`libs`, and `tools`, and `make help` lists the build, smoke test, and Helm targets. The web UI builds with npm in
[`apps/ui`](apps/ui); CI uses Node.js 26.

## Contributing

Read the [contribution guide](CONTRIBUTING.md) before you open a pull request. Backend changes follow
[the backend workflow](docs/design/WORKFLOW.md).

## Releasing Qubership Profiler

Maintainers release from the
[release workflow](https://github.com/Netcracker/qubership-profiler-agent/actions/workflows/release.yaml), and the
[Auto Release workflow](.github/workflows/auto-release.yaml) checks weekly whether a release is due and starts it. See
[Releasing Qubership Profiler](docs/dev/releasing.md) for the steps and the rules for automatic releases.
