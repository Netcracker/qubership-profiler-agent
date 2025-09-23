plugins {
    id("build-logic.java-published-library")
}

dependencies {
    implementation(project(":boot"))
}

tasks.jar {
    manifest {
        attributes["Premain-Class"] = "com.netcracker.profiler.javaagent.Agent"
        attributes["Agent-Class"] = "com.netcracker.profiler.javaagent.Agent"
        attributes["Can-Redefine-Classes"] = "true"
        attributes["Can-Retransform-Classes"] = "true"
        attributes["Boot-Class-Path"] = "qubership-profiler-boot.jar"
    }
}
