plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    testImplementation("com.zaxxer:HikariCP")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.postgresql:postgresql")
}

// postgresql-jdbc.xml also hooks HikariPool.getConnection, and HikariCP 4.0.3 is the last release of
// the line that still runs on Java 8, so the two arms cover both ends of the pool's supported range.
instrumentationTestLibraries(
    "com.zaxxer:HikariCP" to versions("4.0.3"),
)
