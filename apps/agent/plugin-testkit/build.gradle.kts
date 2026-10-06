plugins {
    id("build-logic.java-library")
    id("build-logic.kotlin")
    id("build-logic.test-junit5")
}

description = "Checks a profiler plugin's instrumentation against the real libraries it instruments"

dependencies {
    api(platform(projects.bomTesting))
    api(projects.boot)
    api(projects.instrumenter)
    api(projects.pluginRuntime)
    api("org.junit.jupiter:junit-jupiter-api")
    api("org.junit.jupiter:junit-jupiter-params")
    implementation(kotlin("stdlib"))
    implementation("org.ow2.asm:asm-tree")
    implementation("org.ow2.asm:asm-util")
}
