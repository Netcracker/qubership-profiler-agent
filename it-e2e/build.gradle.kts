import com.github.gradle.node.yarn.task.YarnTask

plugins {
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

val installerZipElements by configurations.dependencyScope("installerZipElements") {
    description = "Declares a dependency on the installer ZIP (javaagent)"
}

val installerZip = configurations.resolvable("installerZip") {
    description = "Resolves the installer ZIP artifact"
    isTransitive = false
    extendsFrom(installerZipElements)
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, "javaagent")
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
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

// Ensure :profiler is evaluated first so we can reference its war task
evaluationDependsOn(":profiler")
val profilerWarFile = project(":profiler").tasks.named<War>("war").flatMap { it.archiveFile }

dependencies {
    installerZipElements(projects.installer)
    testAppJarElements(projects.testApp)
}

// -- Tasks --

val profilerHome = layout.buildDirectory.dir("profiler-home")

val extractInstaller by tasks.registering(Sync::class) {
    from(installerZip.map { zipTree(it.singleFile) })
    into(profilerHome)
}

val dumpDir = layout.buildDirectory.dir("test-dump")
val timestampFile = layout.buildDirectory.file("test-dump-timestamp.txt")

val generateTestDump by tasks.registering(Exec::class) {
    dependsOn(extractInstaller, testAppJar)

    val agentJar = profilerHome.map { it.file("lib/qubership-profiler-agent.jar") }
    val dumpDirPath = dumpDir.map { it.asFile.absolutePath }
    val testAppJarFile = testAppJar.map { it.singleFile }
    val tsFile = timestampFile.map { it.asFile }

    inputs.dir(profilerHome)
    inputs.files(testAppJar)
    outputs.dir(dumpDir)
    outputs.file(timestampFile)
    // Always re-generate the dump so its timestamps fall within "last 15 min"
    outputs.upToDateWhen { false }

    doFirst {
        dumpDir.get().asFile.mkdirs()
        // Record timestamp before running test-app
        tsFile.get().writeText(System.currentTimeMillis().toString())
    }

    executable = "java"
    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-javaagent:${agentJar.get().asFile.absolutePath}",
            "-Dprofiler.dump.home=${dumpDirPath.get()}",
            "-Dcom.netcracker.profiler.agent.LocalBuffer.SIZE=16",
            "-cp", testAppJarFile.get().absolutePath,
            "com.netcracker.profilerTest.testapp.Main",
            "10"
        )
    })
}

tasks.named("yarn_install") {
    inputs.file("package.json").withPropertyName("package_json").withPathSensitivity(PathSensitivity.NONE)
    outputs.file("yarn.lock").withPropertyName("yarn.lock")
}

val playwrightInstall by tasks.registering(YarnTask::class) {
    dependsOn("yarn_install")
    args.addAll("playwright", "install", "chromium")
}

val e2eTest by tasks.registering(YarnTask::class) {
    dependsOn(generateTestDump, playwrightInstall)

    inputs.dir(dumpDir)
    inputs.file(profilerWarFile)
    inputs.dir("e2e").withPropertyName("e2e_tests").withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.file("playwright.config.ts").withPropertyName("playwright_config").withPathSensitivity(PathSensitivity.NONE)

    args.addAll("playwright", "test")

    environment.put("PROFILER_WAR_PATH", profilerWarFile.map { it.asFile.absolutePath })
    environment.put("DUMP_DIR", dumpDir.map { it.asFile.absolutePath })
    environment.put("DUMP_TIMESTAMP", timestampFile.map { it.asFile.readText().trim() })
}
