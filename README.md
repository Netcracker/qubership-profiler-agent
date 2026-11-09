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

* [Liferay](apps/agent/plugins/liferay)

Build systems:

* ANT
  * [ANT (<=1.10.1)](apps/agent/plugins/ant)
  * [ANT (>=1.10.2)](apps/agent/plugins/ant_1102)

Databases:

* DataStax Cassandra
  * [DataStax Cassandra 3.x](apps/agent/plugins/cassandra)
  * [DataStax cassandra 4.x](apps/agent/plugins/cassandra4)
* [ElasticSearch](apps/agent/plugins/elasticsearch)
* [MySQL JDBC](apps/agent/plugins/mysql)
* [PostgeSQL JDBC](apps/agent/plugins/postgresql)

Distribution tracing:

* [Brave (Zipkin agent)](apps/agent/plugins/brave)
* [Jaeger](apps/agent/plugins/jaeger)
* [Ocelot](apps/agent/plugins/ocelot)

HTTP clients:

* [HTTP](apps/agent/plugins/http)
* [Java HTTP Client](apps/agent/plugins/java_http_client)
* [Tomcat <= 9.x](apps/agent/plugins/tomcat_http)
* [Tomcat >= 10.x](apps/agent/plugins/tomcat10_http)
* [Undertow < 2.3](apps/agent/plugins/undertow_http)
* [Undertow >= 2.3](apps/agent/plugins/undertow23_http)

Java Frameworks:

* [Apache Felix](apps/agent/plugins/apache_felix)
* [Equinox](apps/agent/plugins/equinox)
* [Spring Framework](apps/agent/plugins/spring)
  * [Spring REST](apps/agent/plugins/springrest)

Loggers:

* [Log4j](apps/agent/plugins/log4j_enhancer)

Other:

* [Jackson](apps/agent/plugins/jackson)
* [Quartz Scheduler](apps/agent/plugins/quartz)
* [Rhino](apps/agent/plugins/rhino)
* [Test](apps/agent/plugins/test)

Queues:

* [ActiveMQ](apps/agent/plugins/activemq)
* [HornetQ](apps/agent/plugins/hornetq)
* [RabbitMQ](apps/agent/plugins/rabbitmq)

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

The [Auto Release workflow](.github/workflows/auto-release.yaml) runs when the weekly scheduled security scan completes
and on Mondays, and starts the release workflow on `main` when a Dependabot or source code scanning alert has been fixed
since the latest release, or Grype or Trivy finds a HIGH or CRITICAL vulnerability in `latest` that is absent from
`main`. Image comparisons use categories of the form `<image>:<tag>@<platform>` and match the advisory and package
across all locations in each image. Scorecard alerts and code scanning rules without a security severity do not count
as vulnerabilities. Otherwise the workflow releases when about a month has passed since the latest release. It does
nothing when `main` has no commits since the latest release, or while a release workflow run is in progress. The latest
release is the creation time of the newest successful release workflow run on `main`, kept across re-runs, so a fix
merged while a release runs still counts. Without such a run, the latest GitHub release is used. Run it manually with
`dry-run` to see the decision, the alerts it counted, and any skipped image comparisons without releasing.

A Dependabot alert counts only for a runtime dependency outside `apps/examples/` and the `it-*` integration test
directories, and only when the same advisory is not still open for the same package in such a dependency. Alerts on
`settings.gradle.kts` do not count: the Gradle dependency submission sets no scope, and that manifest also covers
build tools such as Checkstyle and Error Prone. Fixes in the Java dependencies therefore ship with the monthly release
unless an image scan finds them in a published image.

When the newest completed release workflow run on `main` did not succeed, for example failed, was cancelled, or timed
out, the Auto Release workflow fails and starts nothing, because that run may have stopped after publishing to Maven
Central, tagging the release, or pushing the version bump. Re-run its failed jobs, or start the release workflow once
the cause is fixed; automatic releases resume after a release workflow run on `main` succeeds. The workflow also skips
the release when a release workflow run starts or finishes while it reads the alerts and images.

Each image pair requires error-free scans of the same image and platform with the same scanner version and
configuration. Both analyses must be for the current `main` commit, at most 48 hours old, and at most 24 hours apart.
Their scan workflows must have completed, and a run whose other jobs failed still counts. They must have started after
the latest release and all runs of the release and backend image build workflows on `main`, including failed runs that
may have published some images. A running publisher, missing or incomplete report, or a branch, image publication, or
release change during comparison invalidates the image comparison. Skipped image comparisons leave Dependabot, source
code scanning, and monthly release decisions available.

To trigger a release, go to the
👉 [Actions tab → release.yaml](https://github.com/Netcracker/qubership-profiler-agent/actions/workflows/release.yaml)
and run it manually.

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
