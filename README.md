# Qubership Profiler Agent

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Netcracker/qubership-profiler-agent/badge)](https://scorecard.dev/viewer/?uri=github.com/Netcracker/qubership-profiler-agent)

This repository container the java agent that can attach as a `-javaagent` to the JVM and collect information
as continuous tracing profiler.

* [Qubership Profiler Agent](#qubership-profiler-agent)
  * [Features](#features)
  * [Library and framework instrumentation](#library-and-framework-instrumentation)
  * [How to build](#how-to-build)
    * [Local build](#local-build)

## Features

* Trace for slow requests and errors
* Continuous profiling
* SQL capture (queries and binds)
* Service call capture

## Library and framework instrumentation

Application servers or Portals:

* [Liferay](apps/plugins/liferay)

Build systems:

* ANT
  * [ANT (<=1.10.1)](apps/plugins/ant)
  * [ANT (>=1.10.2)](apps/plugins/ant_1102)

Databases:

* DataStax Cassandra
  * [DataStax Cassandra 3.x](apps/plugins/cassandra)
  * [DataStax cassandra 4.x](apps/plugins/cassandra4)
* [ElasticSearch](apps/plugins/elasticsearch)
* [MySQL JDBC](apps/plugins/mysql)
* [PostgeSQL JDBC](apps/plugins/postgresql)

Distribution tracing:

* [Brave (Zipkin agent)](apps/plugins/brave)
* [Jaeger](apps/plugins/jaeger)
* [Ocelot](apps/plugins/ocelot)

HTTP clients:

* [HTTP](apps/plugins/http)
* [Java HTTP Client](apps/plugins/java_http_client)
* [Tomcat <= 9.x](apps/plugins/tomcat_http)
* [Tomcat >= 10.x](apps/plugins/tomcat10_http)
* [Undertow < 2.3](apps/plugins/undertow_http)
* [Undertow >= 2.3](apps/plugins/undertow23_http)

Java Frameworks:

* [Apache Felix](apps/plugins/apache_felix)
* [Equinox](apps/plugins/equinox)
* [Spring Framework](apps/plugins/spring)
  * [Spring REST](apps/plugins/springrest)

Loggers:

* [Log4j](apps/plugins/log4j_enhancer)

Other:

* [Jackson](apps/plugins/jackson)
* [Quartz Scheduler](apps/plugins/quartz)
* [Rhino](apps/plugins/rhino)
* [Test](apps/plugins/test)

Queues:

* [ActiveMQ](apps/plugins/activemq)
* [HornetQ](apps/plugins/hornetq)
* [RabbitMQ](apps/plugins/rabbitmq)

## How to build

### Local build

Build requirements:

* Java 17

To build all the artifacts and execute tests, run the following:

```bash
git clone https://github.com/Netcracker/qubership-profiler-agent.git
./gradlew build # builds everything
./gradlew tasks # lists available tasks
```

## Releasing Qubership Profiler

This project defines a [manual release workflow](.github/workflows/release.yaml).

To trigger a release, go to the
👉 [Actions tab → release.yaml](https://github.com/Netcracker/qubership-profiler-agent/actions/workflows/release.yaml) and run it manually.

The release workflow uses [Release Drafter](https://github.com/release-drafter/release-drafter) to prepare
release notes, and it uses labels to group the changes. If you need to adjust the notes, update the labels as needed.

Here's the full step-by-step:
1. Navigate to [Release Workflow](https://github.com/Netcracker/qubership-profiler-agent/actions/workflows/release.yaml)
1. Click on `Run workflow`
1. Select the branch name to be released
1. By default, release workflow would pick the release version from `gradle.properties`, and you can overrided it if needed
1. Click on `Run workflow`

The release workflow would perform the following steps:
1. Check if the release tag `v...` does not exist yet, otherwise it would terminate
1. Bump the version in `gradle.properties` to the release version (e.g., if the manually provided version differs)
1. Build and publish the artifacts to Central Portal
1. Create the release tag and publish GitHub release
1. Bump the version in `gradle.properties` to the next patch version
