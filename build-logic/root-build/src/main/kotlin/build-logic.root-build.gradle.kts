import java.time.Duration

plugins {
    id("build-logic.build-params")
    id("com.gradleup.nmcp.aggregation")
}

nmcpAggregation {
    centralPortal {
        username = providers.environmentVariable("CENTRAL_PORTAL_USERNAME")
        password = providers.environmentVariable("CENTRAL_PORTAL_PASSWORD")
        publishingType = buildParameters.centralPortalPublishingType.name
        validationTimeout = Duration.ofMinutes(buildParameters.centralPortalValidationTimeout.toLong())
    }
}

val githubPackagesPublisher = configurations.dependencyScope("githubPackagesPublisher")
val githubPackagesPublisherClasspath = configurations.resolvable("githubPackagesPublisherClasspath") {
    extendsFrom(githubPackagesPublisher.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        attribute(TargetJvmEnvironment.TARGET_JVM_ENVIRONMENT_ATTRIBUTE, objects.named(TargetJvmEnvironment.STANDARD_JVM))
    }
}

dependencies {
    // Keep the version in sync with the compileOnly dependency in build-logic/root-build/build.gradle.kts
    githubPackagesPublisher("com.gradleup.nmcp:nmcp-tasks:1.6.1")
}

tasks.register<PublishToGithubPackagesTask>("publishAggregationToGithubPackages") {
    group = PublishingPlugin.PUBLISH_TASK_GROUP
    description = "Publishes the aggregation to GitHub Packages of the repository in GITHUB_REPOSITORY"
    repositoryDirectories.from(nmcpAggregation.allFiles)
    // A fork publishes to its own packages, so a workflow run in the fork does not need upstream credentials
    repositoryUrl = providers.gradleProperty("githubPackagesUrl")
        .orElse(
            providers.environmentVariable("GITHUB_REPOSITORY")
                .orElse("Netcracker/qubership-profiler-agent")
                .map { "https://maven.pkg.github.com/$it" }
        )
    username = providers.gradleProperty("githubPackagesUsername")
    password = providers.gradleProperty("githubPackagesPassword")
    publisherClasspath.from(githubPackagesPublisherClasspath)
}
