plugins {
    id("build-logic.java-published-library")
}

dependencies {
    implementation(project(":boot"))
}

tasks.jar {
    manifest {
        attributes["Premain-Class"] = "org.qubership.profiler.javaagent.Agent"
        attributes["Agent-Class"] = "org.qubership.profiler.javaagent.Agent"
        attributes["Can-Redefine-Classes"] = "true"
        attributes["Can-Retransform-Classes"] = "true"
        attributes["Boot-Class-Path"] = "qubership-profiler-boot.jar"
    }
}
