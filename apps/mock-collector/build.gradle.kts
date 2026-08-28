plugins {
    id("build-logic.kotlin-published-library")
}

dependencies {
    api("io.micrometer:micrometer-core")
    implementation(kotlin("stdlib"))
    implementation(projects.protoDefinition)
    implementation(projects.common)
    implementation("org.slf4j:slf4j-api")
    implementation("ch.qos.logback:logback-classic")
}
