plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.inject.extensions:guice-assistedinject")
    implementation("com.google.inject:guice")
    implementation("jakarta.servlet:jakarta.servlet-api")
    implementation("org.jspecify:jspecify")
    implementation("org.openjdk.jmc:common") {
        exclude("org.lz4", "lz4-java")
    }
    implementation("org.openjdk.jmc:flightrecorder") {
        exclude("org.lz4", "lz4-java")
    }
    runtimeOnly("at.yawk.lz4:lz4-java") {
        because("org.openjdk.jmc:common needs lz4 decompressor, and we use at.yawk.lz4:lz4-java to fix CVE-2025-12183")
    }
    testImplementation("org.mockito:mockito-core")
    testImplementation(projects.testkit)
}

// https://github.com/gradle/gradle/pull/16627
private inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val jsResourcesElements = configurations.dependencyScope("jsResourcesElements") {
}

val jsSinglePageResources = configurations.resolvable("jsSinglePageResources") {
    extendsFrom(jsResourcesElements.get())
    attributes {
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.RESOURCES)
        attribute(Attribute.of("com.netcracker.profiler.js.optimization", String::class.java), "single-page")
    }
}

val syncSinglePageResources by tasks.registering(Sync::class) {
    from(jsSinglePageResources)
    into(layout.buildDirectory.dir("generated/single-page"))
}

sourceSets.test {
    resources.srcDir(syncSinglePageResources)
}

dependencies {
    jsResourcesElements(projects.profilerUi)
}
