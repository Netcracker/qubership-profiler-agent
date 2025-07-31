plugins {
    id("build-logic.java-published-library")
//    id("build-logic.jcstress")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
    id("build-logic.kotlin")
    kotlin("kapt")
}

dependencies {
    testImplementation("ch.qos.logback:logback-classic")
    testImplementation("io.mockk:mockk")
    testImplementation("org.openjdk.jcstress:jcstress-core")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testAnnotationProcessor(platform(projects.bomTesting))
    testAnnotationProcessor("org.openjdk.jcstress:jcstress-core")
    testRuntimeOnly(projects.jcstressJupiterEngine)
    kaptTest(platform(projects.bomTesting))
    kaptTest("org.openjdk.jcstress:jcstress-core")
//    jcstressImplementation(platform(projects.bomTesting))
//    jcstressImplementation("org.junit.jupiter:junit-jupiter-api")
//    jcstressAnnotationProcessor(platform(projects.bomTesting))

//    jcstressRuntimeOnly(projects.jcstressJupiterEngine)
//    jcstressRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
//    jcstressRuntimeOnly("org.junit.platform:junit-platform-launcher")
//    jcstressRuntimeOnly(project(":test-config"))
//    jcstressImplementation("org.openjdk.jcstress:jcstress-core")
//    jcstressImplementation("org.junit.platform:junit-platform-commons") {
//        because("We need @Testable annotation")
//    }
}

//val jcstressTest by tasks.registering(Test::class) {
//    classpath = configurations.jcstressRuntimeClasspath.get()
//}

