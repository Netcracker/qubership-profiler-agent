plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
    id("build-logic.kotlin")
    kotlin("kapt")
}

dependencies {
    testImplementation("ch.qos.logback:logback-classic")
    testImplementation("io.mockk:mockk")
    testImplementation("org.openjdk.jcstress:jcstress-core")
    testAnnotationProcessor(platform(projects.bomTesting))
    testAnnotationProcessor("org.openjdk.jcstress:jcstress-core")
    testRuntimeOnly(projects.jcstressJupiterEngine)
    kaptTest(platform(projects.bomTesting))
    kaptTest("org.openjdk.jcstress:jcstress-core")
}
