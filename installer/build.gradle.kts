plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
}

// https://github.com/gradle/gradle/pull/16627
private inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val testApp by sourceSets.creating

val testAppJar by tasks.registering(Jar::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Creates a jar file for a small application for the profiler testing purposes"
    archiveClassifier = "test-app"
    from(testApp.output)
    manifest {
        attributes["Main-Class"] = "com.netcracker.profiler.testapp.Main"
    }
}

// Note: the repository should be cloned under this path externally
// GitHub Actions uses actions/checkout to clone the repository
// For local testing, clone the repo manually
val baseImageRepo = layout.buildDirectory.dir("git/qubership-core-base-images")

val cloneBaseImageRepoIfNeeded by tasks.registering(Exec::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Clones qubership-core-base-images repository if it does not exist locally"
    onlyIf { !baseImageRepo.get().dir(".git").asFile.exists() }
    executable = "git"
    args("clone")
    args("--depth", "50")
    args("--branch", "main")
    args("https://github.com/Netcracker/qubership-core-base-images.git")
    workingDir(baseImageRepo.get().asFile.parentFile)
    outputs.dir(baseImageRepo)
}

val copyInstallerZipToDockerArtifacts by tasks.registering(Copy::class) {
    description =
        "Copies profiler agent distribution to the Docker build directory (Docker can't use files outside of its build directory)"
    dependsOn(cloneBaseImageRepoIfNeeded)
    into(baseImageRepo.map { it.dir("local-artifacts") })
    from(installerZip)
}

val buildBaseImage by tasks.registering(Exec::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Builds java-base image with the latest profiler agent"
    dependsOn(cloneBaseImageRepoIfNeeded, copyInstallerZipToDockerArtifacts)
    executable = "docker"
    workingDir(baseImageRepo)
    args("build")
    args("--file", "Dockerfile.java-alpine")
    args("-t", "qubership/qubership-core-base-image:profiler-latest")
    args("--build-arg", "QUBERSHIP_PROFILER_ARTIFACT_SOURCE=local")
    args("--build-arg", "QUBERSHIP_PROFILER_VERSION=$version")
    args(".")
}

tasks.test {
    dependsOn(buildBaseImage, testAppJar)
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "-Dqubership.profiler.test.java-base.root=${baseImageRepo.get().asFile.absolutePath}",
                "-Dqubership.profiler.testapp.jar=${testAppJar.get().archiveFile.get().asFile.absolutePath}",
            )
        }
    )
}

val installerZipElements by configurations.dependencyScope("installerZipElements") {
}

val installerZipFiles = configurations.resolvable("installerZipFiles") {
    isTransitive = false
    extendsFrom(installerZipElements)
    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.JAR)
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.EXTERNAL)
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
}

dependencies {
    testImplementation("org.testcontainers:junit-jupiter")
    installerZipElements(projects.boot)
    installerZipElements(projects.agent)
    installerZipElements(projects.runtime) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.SHADOWED)
        }
    }
    installerZipElements(projects.plugins.activemq)
    installerZipElements(projects.plugins.ant)
    installerZipElements(projects.plugins.ant1102)
    installerZipElements(projects.plugins.apacheFelix)
    installerZipElements(projects.plugins.apacheHttpclient)
    installerZipElements(projects.plugins.brave)
    installerZipElements(projects.plugins.cassandra)
    installerZipElements(projects.plugins.cassandra4)
    installerZipElements(projects.plugins.couchbase)
    installerZipElements(projects.plugins.elasticsearch)
    installerZipElements(projects.plugins.equinox)
    installerZipElements(projects.plugins.hornetq)
    installerZipElements(projects.plugins.http)
    installerZipElements(projects.plugins.jackson)
    installerZipElements(projects.plugins.jaeger)
    installerZipElements(projects.plugins.javaHttpClient)
    installerZipElements(projects.plugins.jettyHttp)
    installerZipElements(projects.plugins.jetty10Http)
    installerZipElements(projects.plugins.liferay)
    installerZipElements(projects.plugins.log4jEnhancer)
    installerZipElements(projects.plugins.mysql)
    installerZipElements(projects.plugins.ocelot)
    installerZipElements(projects.plugins.postgresql)
    installerZipElements(projects.plugins.quartz)
    installerZipElements(projects.plugins.rabbitmq)
    installerZipElements(projects.plugins.rhino)
    installerZipElements(projects.plugins.spring)
    installerZipElements(projects.plugins.springrest)
    installerZipElements(projects.plugins.tomcatHttp)
    installerZipElements(projects.plugins.tomcat10Http)
    installerZipElements(projects.plugins.undertowHttp)
    installerZipElements(projects.plugins.undertow23Http)
    installerZipElements(projects.plugins.vertx)
}

val installerZip by tasks.registering(Zip::class) {
    from(sourceSets.main.get().resources)
    into("lib") {
        from(installerZipFiles) {
            // Remove "-$version" suffix from the jar names as the profiler does not expect files with versions
            rename("""-\d+\.\d+(?:.(?!jar$))+""", "")
        }
    }
    // TODO: do we really need extracting the config files? Could the agent look into the jars?
    into(".") {
        includeEmptyDirs = false
        from(
            installerZipFiles.get().elements.map { jars ->
                jars.map { jar ->
                    zipTree(jar)
                        .matching {
                            include("config/**/*.xml")
                        }
                }
            }
        )
    }
}

// Publish installer.zip to Maven repository so users could consume
val javaagent = configurations.consumable("javaagent") {
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, "javaagent")
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
    outgoing {
        artifact(installerZip)
    }
}

(components["java"] as AdhocComponentWithVariants).addVariantsFromConfiguration(javaagent.get()) {
}
