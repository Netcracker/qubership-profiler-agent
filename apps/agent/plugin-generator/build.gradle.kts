plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    implementation("com.github.ajalt.clikt:clikt")
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
    implementation("org.slf4j:slf4j-api")
    implementation(projects.pluginRuntime)
}
