plugins {
    id("build-logic.java-published-library")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.parsers)
    api("jakarta.servlet:jakarta.servlet-api")
    implementation("ch.qos.logback:logback-classic")
    implementation("ch.qos.logback:logback-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.google.inject.extensions:guice-servlet")
    implementation("com.google.inject:guice")
    implementation("org.openjdk.jmc:common") {
        exclude("org.lz4", "lz4-java")
    }
    implementation("org.openjdk.jmc:flightrecorder") {
        exclude("org.lz4", "lz4-java")
    }
    runtimeOnly("at.yawk.lz4:lz4-java") {
        because("org.openjdk.jmc:common needs lz4 decompressor, and we use at.yawk.lz4:lz4-java to fix CVE-2025-12183")
    }
}
