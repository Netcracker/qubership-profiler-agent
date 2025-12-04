plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.boot)
    api("org.slf4j:slf4j-api")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.google.inject:guice")
    implementation("com.jcraft:jsch")
    implementation("net.sf.trove4j:trove4j")
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
}
