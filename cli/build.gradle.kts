plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-junit5")
    id("build-logic.test-jmockit")
}

dependencies {
    implementation(projects.common)
    implementation(projects.parsers)
    implementation(projects.web)
    implementation("backport-util-concurrent:backport-util-concurrent")
    implementation("ch.qos.logback:logback-classic")
    implementation("ch.qos.logback:logback-core")
    implementation("net.sourceforge.argparse4j:argparse4j")
    runtimeOnly("javax:javaee-api")
}

tasks.jar {
    manifest {
        attributes["Class-Path"] = "../runtime.jar"
        attributes["Main-Class"] = "com.netcracker.profiler.cli.Main"
    }
}
