import com.github.vlsi.gradle.dsl.configureEach

plugins {
    id("checkstyle")
    id("build-logic.toolchains")
    id("build-logic.build-params")
}

checkstyle {
    toolVersion = "10.25.0"
    providers.gradleProperty("checkstyle.version")
        .takeIf { it.isPresent }
        ?.let { toolVersion = it.get() }

    // Current one is ~8.8
    // https://github.com/julianhyde/toolbox/issues/3
    isShowViolations = true
    // TOOD: move to /config
    configDirectory = layout.settingsDirectory.dir(".github/linters")
    config = resources.text.fromFile(configDirectory.file("checkstyle.xml"))
    configProperties = mapOf(
        "cache_file" to layout.buildDirectory.dir("checkstyle/cacheFile").get().asFile.relativeTo(configDirectory.get().asFile),
    )
}

val checkstyleTasks = tasks.withType<Checkstyle>()
checkstyleTasks.configureEach {
    // Checkstyle 8.26 does not need classpath, see https://github.com/gradle/gradle/issues/14227
    classpath = files()
}

tasks.register("checkstyleAll") {
    dependsOn(checkstyleTasks)
}

plugins.withId("java")  {
    tasks.configureEach<Checkstyle> {
        buildParameters.buildJdk?.let {
            javaLauncher.convention(project.the<JavaToolchainService>().launcherFor(it))
        }
    }
}
