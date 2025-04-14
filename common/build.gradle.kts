plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.boot)
    api("org.slf4j:slf4j-api")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("commons-lang:commons-lang")
    implementation("net.sf.trove4j:trove4j")
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
    implementation("org.springframework.boot:spring-boot")
    testImplementation("commons-io:commons-io")
}
