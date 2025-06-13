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
}

val profilerHome = layout.buildDirectory.dir("profiler-home")

val extractInstaller by tasks.registering(Sync::class) {
    into(profilerHome)
    from(installerZip.get().elements.map { zips -> zips.map { zipTree(it) } })
}

tasks.test {
    dependsOn(extractInstaller)
    systemProperty("org.qubership.profiler.agent.LocalBuffer.SIZE", "16")
    // Execute tests with profiler
    val dumpHome = layout.buildDirectory.dir("dump")
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "-javaagent:${profilerHome.get().asFile.absolutePath}/lib/qubership-profiler-agent.jar",
                "-Dprofiler.dump.home=${dumpHome.get().asFile.absolutePath}",
            )
        }
    )
}
