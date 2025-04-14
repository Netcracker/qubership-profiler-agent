plugins {
    id("build-logic.java-library")
    id("build-logic.test-testng")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.instrumenter)
    implementation(projects.plugins.test)
    testImplementation("junit:junit")
    testImplementation("org.springframework:spring-test")
}
