plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used for testing purposes by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        api("com.beust:jcommander:1.82")
        api("junit:junit:4.13.2")
        api("org.jmockit:jmockit-coverage:1.23")
        api("org.jmockit:jmockit:1.49")
        api("org.mockito:mockito-core:5.18.0")
    }
}
