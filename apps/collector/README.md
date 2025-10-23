# cdt-cloud-profiler

Execution Statistics Collector (Cloud Profiler) to Quarkus 3

## TODO

- (build) Jenkins build should be run integration tests (with `TestContainers`)
- (release) RELEASE NOTES! installation/migration guide

### Migration from ESC

1. services migration:
   1. keep old `ui-service` (at least, 2 weeks to read old data)
   2. `collector-service` - delete or scale->0
   3. `static-service` (nginx) - delete or scale->0
      -> route `collector-service`@kube should point out to new `collector` service
   4. delete old external ingress/routes?

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```bash
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at `http://localhost:8080/q/dev/`.

## Packaging and running the application

The application can be packaged using:

```bash
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/app/lib/` directory.

The application is now runnable using `java -jar target/app/quarkus-run.jar`.

## Creating a native executable

You can create a native executable using:

```bash
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```bash
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/cdt-cloud-profiler-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult
[https://quarkus.io/guides/maven-tooling](https://quarkus.io/guides/maven-tooling).
