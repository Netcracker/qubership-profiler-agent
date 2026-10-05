plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    injectorImplementation("jakarta.servlet:jakarta.servlet-api:6.0.0")
}

// 2.3.0.Final is the first 2.3 release, and bom-testing tracks the latest one, so the two arms of
// this test are the two ends of the 2.3 line rather than the same version twice.
instrumentationTestLibraries(
    "io.undertow:undertow-servlet" to versions("2.3.0.Final"),
)
