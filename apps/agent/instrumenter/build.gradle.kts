plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.pluginRuntime)
    implementation("ch.qos.logback:logback-classic")
    implementation("ch.qos.logback:logback-core")
    implementation("javax:javaee-api")
    implementation("net.sf.trove4j:trove4j")
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
    implementation("org.slf4j:slf4j-api")
}
