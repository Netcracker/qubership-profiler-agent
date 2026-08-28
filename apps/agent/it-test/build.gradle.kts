plugins {
    id("build-logic.java-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
    id("java-test-fixtures")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.instrumenter)
    implementation(projects.plugins.test)
    testFixturesImplementation(projects.boot)
    testFixturesImplementation(projects.common)
}
