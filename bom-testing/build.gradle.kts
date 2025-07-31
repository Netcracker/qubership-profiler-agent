plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used for testing purposes by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("org.junit:junit-bom:5.13.4"))
    constraints {
        api("com.beust:jcommander:1.82")
        api("io.mockk:mockk:1.14.5")
        api("org.jmockit:jmockit-coverage:1.23")
        api("org.jmockit:jmockit:1.50")
        api("org.mockito:mockito-core:5.18.0")
        api("org.openjdk.jcstress:jcstress-core:0.16")
        api("org.testcontainers:junit-jupiter:1.21.3")
    }
}
