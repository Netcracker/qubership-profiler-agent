plugins {
    id("build-logic.kotlin")
}

dependencies {
    api(platform(projects.bomTesting))
    implementation(kotlin("stdlib"))
    api("org.testcontainers:testcontainers-junit-jupiter")
}
