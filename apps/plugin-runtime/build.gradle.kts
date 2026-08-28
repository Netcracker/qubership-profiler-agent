plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
}
