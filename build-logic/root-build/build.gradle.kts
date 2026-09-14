plugins {
    id("build-logic.kotlin-dsl-gradle-plugin")
}

dependencies {
    implementation(project(":build-parameters"))
    implementation("com.gradleup.nmcp.aggregation:com.gradleup.nmcp.aggregation.gradle.plugin:1.6.1")
    // PublishToGithubPackagesTask loads nmcp-tasks in an isolated classloader at execution time,
    // so the plugin classpath does not get okhttp, coroutines, and kotlinx-serialization.
    // Keep the version in sync with the githubPackagesPublisher dependency in build-logic.root-build.gradle.kts.
    compileOnly("com.gradleup.nmcp:nmcp-tasks:1.6.1")
    compileOnly("com.squareup.okio:okio-jvm:3.17.0")
}
