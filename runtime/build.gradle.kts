plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
    id("com.gradleup.shadow")
}

dependencies {
    implementation(projects.dumper)
    implementation(projects.instrumenter)
}

tasks.shadowJar {
    manifest {
        attributes["Entry-Points"] = "org.qubership.profiler.agent.plugins.EnhancerRegistryPluginImpl org.qubership.profiler.agent.plugins.ProfilerTransformerPluginImpl org.qubership.profiler.agent.plugins.DumperPluginImpl"
    }
}

val extraMavenPublications by configurations.getting

// (artifacts) {
//    extraMavenPublications(tasks.shadowJar) {
//        classifier = ""
//    }
// }
