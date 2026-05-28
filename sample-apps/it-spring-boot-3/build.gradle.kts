import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    id("build-logic.java-17-library")
    id("build-logic.kotlin")
    id("build-logic.test-junit5")
}

tasks.withType<KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}

dependencies {
    val testcontainersBom = enforcedPlatform("org.testcontainers:testcontainers-bom:1.21.0")
    testImplementation(testcontainersBom)
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
}

val demoModuleDir = rootDir.resolve("backend/examples/spring-boot-3-undertow")
val demoTargetDir = demoModuleDir.resolve("target")

// Build the existing Maven demo (Spring Boot 3 + Undertow + Spring Session). The demo doubles
// as the test fixture — same source code shipped as a deployable sample. The test packages this
// jar on top of qubership/qubership-core-base-image:profiler-latest, which already has the
// profiler agent baked into /app/diag and wires -javaagent via its entrypoint when
// NC_DIAGNOSTIC_MODE=prod.
val buildDemoApp by tasks.registering(Exec::class) {
    workingDir = demoModuleDir
    executable = "mvn"
    args("package", "-DskipTests", "-q")

    inputs.file(demoModuleDir.resolve("pom.xml"))
    inputs.dir(demoModuleDir.resolve("src"))
    outputs.dir(demoTargetDir)
}

val demoJarProvider = buildDemoApp.map {
    fileTree(demoTargetDir) {
        include("*.jar")
        exclude("original-*", "*-sources.jar", "*-javadoc.jar")
    }.singleFile
}

tasks.test {
    dependsOn(buildDemoApp, ":installer:buildBaseImage")

    inputs.files(demoTargetDir).withPropertyName("demoTarget").withPathSensitivity(PathSensitivity.RELATIVE)

    systemProperty("test.baseImageTag", "qubership/qubership-core-base-image:profiler-latest")

    // docker-java 3.x bundled with Testcontainers 1.21 advertises Docker API 1.32 by default,
    // which modern Docker daemons (OrbStack, Docker Desktop ≥ 25) reject with
    // "client version 1.32 is too old". Force a newer negotiated API version.
    systemProperty("api.version", "1.43")

    // Resolve the demo jar lazily, at execution time: buildDemoApp populates target/ only when it
    // runs, so reading demoJarProvider.get() during configuration fails on a clean checkout
    // (empty target/). A CommandLineArgumentProvider defers it until the command line is built.
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf("-Dtest.demoAppJar=${demoJarProvider.get().absolutePath}")
        },
    )
}
