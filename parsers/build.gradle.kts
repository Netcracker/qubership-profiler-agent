plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("commons-lang:commons-lang")
    implementation("javax:javaee-api")
    implementation("org.openjdk.jmc:common")
    implementation("org.openjdk.jmc:flightrecorder")
    implementation("org.springframework:spring-context")
    testImplementation("junit:junit")
    testImplementation("org.mockito:mockito-core")
}
