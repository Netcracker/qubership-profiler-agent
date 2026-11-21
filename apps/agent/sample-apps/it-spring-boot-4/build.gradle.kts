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

// https://github.com/gradle/gradle/pull/16627
private inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val installerZipElements = configurations.dependencyScope("installerZipElements")

// The agent the base images carry. The images are tagged by name, so the test declares the installer as an input
// to rerun when an agent or plugin change rebuilds the images.
val installerZip = configurations.resolvable("installerZip") {
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, "javaagent")
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.testJdkVersion)
    }
    extendsFrom(installerZipElements.get())
}

dependencies {
    installerZipElements(projects.installer)
    val testcontainersBom = enforcedPlatform("org.testcontainers:testcontainers-bom:2.0.5")
    testImplementation(testcontainersBom)
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    // DumpTrace reads the agent's dump back as TSV
    testImplementation(projects.cli)
    testImplementation(projects.parsers)
    testImplementation("com.google.inject:guice")
    testImplementation("net.sourceforge.argparse4j:argparse4j")
}

val demoModuleDir = rootDir.resolve("apps/examples/spring-boot-4-tomcat")
val demoTargetDir = demoModuleDir.resolve("target")

// The example compiles with release 21, and Gradle itself may run on JDK 17
val demoJavaLauncher = javaToolchains.launcherFor {
    languageVersion = JavaLanguageVersion.of(21)
}

// Build the Maven example (Spring Boot 4 + Tomcat + JDBC + RestTemplate). The example doubles
// as the test fixture: the test layers the jar on top of the profiler base images.
val buildDemoApp by tasks.registering(Exec::class) {
    workingDir = demoModuleDir
    executable = "mvn"
    args("package", "-DskipTests", "-q")

    inputs.file(demoModuleDir.resolve("pom.xml"))
    inputs.dir(demoModuleDir.resolve("src"))
    outputs.dir(demoTargetDir)

    val javaHome = demoJavaLauncher.map { it.metadata.installationPath.asFile.absolutePath }
    inputs.property("javaHome", javaHome)
    doFirst {
        environment("JAVA_HOME", javaHome.get())
    }
}

val demoJarProvider = buildDemoApp.map {
    fileTree(demoTargetDir) {
        include("*.jar")
        exclude("original-*", "*-sources.jar", "*-javadoc.jar")
    }.singleFile
}

tasks.test {
    dependsOn(buildDemoApp, ":installer:buildBaseImage", ":installer:buildBaseImageJdk25")

    inputs.files(demoTargetDir).withPropertyName("demoTarget").withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.files(installerZip).withPropertyName("installerZip").withPathSensitivity(PathSensitivity.NONE)

    systemProperty("test.baseImageTag.jdk21", "qubership/qubership-core-base-image:profiler-latest")
    systemProperty("test.baseImageTag.jdk25", "qubership/qubership-core-base-image:profiler-jdk25-latest")

    // Modern Docker daemons reject the default API version docker-java advertises
    systemProperty("api.version", "1.43")

    // Resolve the demo jar lazily, at execution time: buildDemoApp populates target/ only when it runs
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf("-Dtest.demoAppJar=${demoJarProvider.get().absolutePath}")
        },
    )
}
