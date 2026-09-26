plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used for testing purposes by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("org.junit:junit-bom:5.14.4"))
    api(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    constraints {
        api("com.beust:jcommander:1.82")
        // The moving version each plugin instrumentation test checks its injectors against. The
        // versions held still beside it live in plugins/*/build.gradle.kts, which Renovate ignores.
        api("com.rabbitmq:amqp-client:5.35.0")
        api("com.zaxxer:HikariCP:7.1.0")
        api("io.mockk:mockk:1.14.11")
        api("io.undertow:undertow-servlet:2.3.26.Final")
        api("org.jmockit:jmockit-coverage:1.23")
        api("org.jmockit:jmockit:1.50")
        api("org.mockito:mockito-core:5.23.0")
        api("org.openjdk.jcstress:jcstress-core:0.16")
        api("org.postgresql:postgresql:42.7.13")
        api("org.springframework.amqp:spring-rabbit:4.1.1")
    }
}
