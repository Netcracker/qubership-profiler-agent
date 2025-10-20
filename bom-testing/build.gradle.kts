plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used for testing purposes by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("org.junit:junit-bom:5.13.4"))
    api(platform("org.testcontainers:testcontainers-bom:1.21.3"))
    constraints {
        api("com.beust:jcommander:1.82")
        api("com.zaxxer:HikariCP:7.0.2")
        api("io.mockk:mockk:1.14.5")
        api("org.jmockit:jmockit-coverage:1.23")
        api("org.jmockit:jmockit:1.50")
        api("org.mockito:mockito-core:5.20.0")
        api("org.openjdk.jcstress:jcstress-core:0.16")
        api("org.postgresql:postgresql:42.7.8")
    }
}
