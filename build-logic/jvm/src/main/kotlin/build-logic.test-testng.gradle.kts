import com.github.vlsi.gradle.dsl.configureEach

plugins {
    id("java-library")
    id("build-logic.build-params")
    id("build-logic.test-base")
}

dependencies {
    testImplementation("org.testng:testng:7.11.0")
}

tasks.configureEach<Test> {
    useTestNG()
}

tasks.withType<JavaCompile>()
    .matching { it.name.contains("Test") }
    .configureEach {
        // TestNG requires Java 11+
        options.release.set(buildParameters.targetJavaVersion.coerceAtLeast(11))
    }
