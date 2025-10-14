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
