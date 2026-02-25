import com.github.gradle.node.yarn.task.YarnTask
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

plugins {
    id("base")
    id("build-logic.repositories")
    id("com.github.node-gradle.node")
}

node {
    version = "22.14.0"
    download = true
}

// https://github.com/gradle/gradle/pull/16627
private inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

// -- Artifact resolution --

val profilerWarElements by configurations.dependencyScope("profilerWarElements") {
    description = "Declares a dependency on the profiler WAR"
}

val profilerWar = configurations.resolvable("profilerWar") {
    description = "Resolves profiler WAR"
    extendsFrom(profilerWarElements)
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, "war")
    }
}

val testAppJarElements = configurations.dependencyScope("testAppJarElements") {
    description = "Declares a dependency for test application"
}

val testAppJar = configurations.resolvable("testAppJar") {
    description = "Resolves test application JAR"
    extendsFrom(testAppJarElements.get())
    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.JAR)
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.EXTERNAL)
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
}

val coreBaseImageTag = "qubership/qubership-core-base-image:profiler-latest"

dependencies {
    testAppJarElements(projects.testApp)
    profilerWarElements(projects.profiler)
}

// -- Tasks --

val dumpDir = layout.buildDirectory.dir("test-dump")
val profilerConfigFile = layout.projectDirectory.file("config/_config.xml")

val cleanDumpDir by tasks.registering(Delete::class) {
    delete(dumpDir)
}

val generateTestDump by tasks.registering(Exec::class) {
    dependsOn(cleanDumpDir, ":installer:buildBaseImage", testAppJar)

    inputs.property("coreBaseImageTag", coreBaseImageTag)
    inputs.files(testAppJar).withPropertyName("test_app_jar").withPathSensitivity(PathSensitivity.NONE)
    inputs.file(profilerConfigFile).withPropertyName("profiler_config").withPathSensitivity(PathSensitivity.NONE)
    outputs.dir(dumpDir).withPropertyName("dump_dir")
    // Always re-generate the dump so its timestamps fall within "last 15 min"
    outputs.upToDateWhen { false }

    executable = "docker"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "run",
            "--rm",
            "--volume", "${dumpDir.get().asFile.absolutePath}:/app/diag/localdump",
            "--volume", "${testAppJar.get().singleFile.absolutePath}:/app/testapp.jar:ro",
            "--volume", "${profilerConfigFile.asFile.absolutePath}:/app/diag/config/default/50main/testapp.xml:ro",
            "--env", "NC_DIAGNOSTIC_MODE=prod",
            coreBaseImageTag,
            "java",
            // explicitly add podname otherwise bootstrap scripts miss it
            "-Dprofiler.dump=/app/diag/localdump/podname",
            "-Dcom.netcracker.profiler.Dumper.REMOTE_DUMP_DISABLED=true",
            "-jar",
            "/app/testapp.jar",
            "10"
        )
    })
    doFirst {
        val dumpDir = dumpDir.get().asFile
        // Container runs with a different user than the host OS, so the container can't write files to the dump dir
        // by default
        Files.setPosixFilePermissions(
            dumpDir.toPath(),
            PosixFilePermissions.fromString("rwxrwxrwx")
        )
    }
}

tasks.named("yarn_install") {
    inputs.file("package.json").withPropertyName("package_json").withPathSensitivity(PathSensitivity.NONE)
    outputs.file("yarn.lock").withPropertyName("yarn.lock")
}

val playwrightInstall by tasks.registering(YarnTask::class) {
    dependsOn("yarn_install")
    args.addAll("playwright", "install", "chromium")
}

val runProfiler by tasks.registering(JavaExec::class) {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Generates test dump and starts the profiler UI for manual inspection"
    dependsOn(generateTestDump)

    classpath = files(profilerWar)
    jvmArgs("-Dprofiler.dump=${dumpDir.get().asFile.absolutePath}")
    args("--httpPort", "18090")
}

val e2eTest by tasks.registering(YarnTask::class) {
    dependsOn(generateTestDump, playwrightInstall)

    inputs.dir(dumpDir).withPropertyName("dump_dir").withPathSensitivity(PathSensitivity.NONE)
    inputs.files(profilerWar).withPropertyName("profiler_war").withPathSensitivity(PathSensitivity.NONE)
    inputs.dir("e2e").withPropertyName("e2e_tests").withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.file("playwright.config.ts").withPropertyName("playwright_config").withPathSensitivity(PathSensitivity.NONE)

    args.addAll("playwright", "test")

    environment.put("PROFILER_WAR_PATH", profilerWar.map { it.singleFile.absolutePath })
    environment.put("DUMP_DIR", dumpDir.map { it.asFile.absolutePath })
}

val check by tasks.existing {
    dependsOn(e2eTest)
}
