plugins {
    id("build-logic.java-library")
    id("build-logic.kotlin")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
}

val installerZipElements = configurations.dependencyScope("installerZipElements")

// https://github.com/gradle/gradle/pull/16627
private inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val otelJavaagentElements = configurations.dependencyScope("otelJavaagentElements")

val otelJavaagent = configurations.resolvable("otelJavaagent") {
    extendsFrom(otelJavaagentElements.get())
}

val installerZip = configurations.resolvable("installerZip") {
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, "javaagent")
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.testJdkVersion)
    }
    extendsFrom(installerZipElements.get())
}

dependencies {
    installerZipElements(projects.installer)
    testCompileOnly(projects.boot)
    // Drives the minimal logback-visibility reproducer: the test reconfigures Logback the way a
    // host application (e.g. Spring Boot) would, then checks whether the agent's plugin logger
    // still reaches that configuration.
    testImplementation("ch.qos.logback:logback-classic")
    // Runs under the agent, so the opentelemetry plugin instruments it as it would in an application
    testImplementation("io.opentelemetry:opentelemetry-sdk-trace")
    testImplementation("io.jaegertracing:jaeger-core")
    testImplementation("io.zipkin.brave:brave")
    otelJavaagentElements(platform(projects.bomTesting))
    otelJavaagentElements("io.opentelemetry.javaagent:opentelemetry-javaagent")
}

val profilerHome = layout.buildDirectory.dir("profiler-home")

val extractInstaller by tasks.registering(Sync::class) {
    into(profilerHome)
    from(installerZip.get().elements.map { zips -> zips.map { zipTree(it) } })
}

// The tests that need the OpenTelemetry Java agent beside the profiler. They run in a task of their
// own, because the agent changes what every other test in the JVM sees.
val otelJavaagentTag = "otel-javaagent"

fun Test.attachProfiler(dumpDirName: String) {
    dependsOn(extractInstaller)
    systemProperty("com.netcracker.profiler.agent.LocalBuffer.SIZE", "16")
    // Execute tests with profiler
    val dumpHome = layout.buildDirectory.dir(dumpDirName)
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "-javaagent:${profilerHome.get().asFile.absolutePath}/lib/qubership-profiler-agent.jar",
                "-Dprofiler.dump.home=${dumpHome.get().asFile.absolutePath}",
            )
        }
    )
}

tasks.test {
    attachProfiler("dump")
    useJUnitPlatform {
        excludeTags(otelJavaagentTag)
    }
}

val otelJavaagentTest by tasks.registering(Test::class) {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Runs the tests that need the profiler and the OpenTelemetry Java agent attached together"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    // The profiler goes first: it has to register its transformer before the OpenTelemetry agent
    // loads the context classes the opentelemetry plugin instruments.
    attachProfiler("dump-otel-javaagent")
    useJUnitPlatform {
        includeTags(otelJavaagentTag)
    }
    val javaagent = otelJavaagent.get()
    inputs.files(javaagent).withPropertyName("otelJavaagent").withNormalizer(ClasspathNormalizer::class)
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "-javaagent:${javaagent.files.single { it.name.startsWith("opentelemetry-javaagent-") }.absolutePath}",
                // Nothing listens for telemetry in a test run
                "-Dotel.traces.exporter=none",
                "-Dotel.metrics.exporter=none",
                "-Dotel.logs.exporter=none",
            )
        }
    )
}

tasks.check {
    dependsOn(otelJavaagentTest)
}
