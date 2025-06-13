plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
    id("build-logic.kotlin")
}

dependencies {
    testImplementation("ch.qos.logback:logback-classic")
    testImplementation("io.mockk:mockk")
}
