plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.parsers)
    implementation(projects.protoDefinition)
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("commons-lang:commons-lang:2.6")
    implementation("net.sf.trove4j:trove4j")
    implementation("org.apache.httpcomponents:httpcore")
    implementation("org.hdrhistogram:HdrHistogram")
}
