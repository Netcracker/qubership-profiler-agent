plugins {
    id("build-logic.kotlin")
    application
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(projects.protoDefinition)
    implementation(projects.common)
    implementation("org.slf4j:slf4j-api")
    implementation("ch.qos.logback:logback-classic")
    implementation("org.apache.commons:commons-lang3")
}

application {
    mainClass.set("com.netcracker.profiler.collector.mock.MockCollectorMainKt")
}

// Docker image build task for testing
val buildMockCollectorDockerImage by tasks.registering(Exec::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Builds Docker image for mock-collector (for integration tests)"
    dependsOn(tasks.installDist)

    executable = "docker"
    args("build")
    args("-t", "qubership/mock-collector:test")
    args("-f", "Dockerfile")
    args(".")

    workingDir = projectDir

    // Make sure this runs after installDist completes
    inputs.dir(layout.buildDirectory.dir("install/mock-collector"))
    outputs.upToDateWhen { false } // Always rebuild for tests
}
