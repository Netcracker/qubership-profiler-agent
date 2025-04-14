plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
    implementation("org.slf4j:slf4j-api")
}
