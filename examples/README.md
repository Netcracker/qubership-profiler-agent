# profiler-examples

This repository contains simple examples of services that can:

* Collect and expose metrics
* Contains a profiler agent that will attach to the process
* Use different frameworks

| Application              | Base image               | Framework     | Java SDK | Description |
|--------------------------|--------------------------|---------------|----------|-------------|
| quarkus-2-vertx          | openjdk11-x86:11.0.23.01 | 2.16.10.Final | 11       |             |
| quarkus-3-vertx          | openjdk17:17.0.11.9.04   | 3.3.0         | 17       |             |
| quarkus-3-vertx-resteasy | openjdk21:21.0.3.9.04    | 3.3.0         | 21       |             |
| spring-boot-2-jetty      | openjdk21:21.0.3.9.04    | 2.7.12        | 21       |             |
| spring-boot-2-tomcat     | openjdk17:17.0.11.9.04   | 2.7.12        | 17       |             |
| spring-boot-2-undertow   | openjdk11-x86:11.0.23.01 | 2.7.12        | 11       |             |
| spring-boot-3-jetty      | openjdk17:17.0.11.9.04   | 3.1.2         | 17       |             |
| spring-boot-3-tomcat     | openjdk17:17.0.11.9.04   | 3.2.4         | 17       |             |
| spring-boot-3-undertow   | openjdk21:21.0.3.9.04    | 3.1.2         | 21       |             |

Base images:

* 11: centos7/openjdk11-x86:11.0.23.01
* 17: alpine/openjdk17:17.0.11.9.04
* 21: alpine/openjdk21:21.0.3.9.04

See also end of life dates for [Spring Boot](https://endoflife.date/spring-boot)
and [Quarkus](https://endoflife.date/quarkus-framework) frameworks.

## Endpoints

| Endpoint                      | Parameter             | Framework | Slow? | Description                    |
|-------------------------------|-----------------------|-----------|-------|--------------------------------|
| GET `/custom/err`             |                       | Both      | Yes   | Update `response.size` metrics | 
| GET `/custom/health`          |                       | Both      | Yes   |                                |
| POST `/custom/health?status=` | string (`UP`, `DOWN`) | Both      | No    |                                | 
| GET `/custom/gauge/{number}`  | long                  | Quarkus   | No    |                                |
| GET `/custom/prime/{number}`  | long (should be >0)   | Quarkus   | Yes   | Check if number is prime       |
| GET `/memory/{mb}`            | int                   | Quarkus   | Yes   | Generate n-th Mb of data       |
| GET `/memory/oom`             |                       | Quarkus   | Yes   | OutOfMemory                    |
| GET `/q/metrics`              |                       | Quarkus   | No    | Prometheus endpoint            |
| GET `/metrics`                |                       | Spring    | No    | Prometheus endpoint            |

Also, a cron job (each `10s`) randomly calls one of these endpoints.

## Spring Boot build

To build an application, you have to execute a command:

```bash
mvn clean install
```

To build after it a docker image:

```bash
docker build .
```

To update agent to the custom version just copy `installer-cloud.zip` to the `/agent` directory
and build a docker image.

If you want to use in-built in the image agent, you can remove a section:

```dockerfile
### Star update agent inside the image
ENV APP_UID=10001

USER root

COPY agent/installer-cloud-*.zip /tmp/installer-cloud.zip

# Replace in-built archive with profiler agent to latest version
RUN rm -rf /app/volumes/ncdiag/config/* \
    && unzip -oq /tmp/installer-cloud.zip -d /app/volumes/ncdiag \
    && rm -rf /tmp/installer-cloud.zip \
    && chmod a+rwx -R /app/volumes/ncdiag \
    && chown -R ${APP_UID} /app/volumes/ncdiag

USER ${APP_UID}
### End
```

from the `Dockerfile`.

## Quarkus build

There are three options to build Quarkus applications:

* in fast-jar
* in legacy-jar/fat-jar
* in native binary

**Note:** Instead of using `mvn` for Quarkus builds you can use maven wrapper `mvnw`.

To build in `fast-jar` need to run:

```bash
mvn clean package
```

To build as `legay-jar` need to run the following command:

```bash
mvn clean package -P legacy
```

To build as `native` need to run the following command:

```bash
mvn clean package -P native
```

To build after it a docker image with `fast-jar`:

```bash
docker build . -f docker/Dockerfile.jvm
```

To build after it a docker image with `legacy-jar`:

```bash
docker build . -f docker/Dockerfile.legacy-jar
```

To build after it a docker image with `native`:

```bash
docker build . -f docker/Dockerfile.native
```

To update agent to the custom version just copy `installer-cloud.zip` to the `/agent` directory
and build a docker image.

If you want to use in-built in the image agent, you can remove a section:

```dockerfile
### Star update agent inside the image
ENV APP_UID=10001

USER root

COPY agent/installer-cloud-*.zip /tmp/installer-cloud.zip

# Replace in-built archive with profiler agent to latest version
RUN rm -rf /app/volumes/ncdiag/config/* \
    && unzip -oq /tmp/installer-cloud.zip -d /app/volumes/ncdiag \
    && rm -rf /tmp/installer-cloud.zip \
    && chmod a+rwx -R /app/volumes/ncdiag \
    && chown -R ${APP_UID} /app/volumes/ncdiag

USER ${APP_UID}
### End
```

from the `Dockerfile`.
