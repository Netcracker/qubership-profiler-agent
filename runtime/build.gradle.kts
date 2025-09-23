plugins {
    id("build-logic.java-published-library")
    id("com.gradleup.shadow")
}

dependencies {
    implementation(projects.dumper)
    implementation(projects.instrumenter)
}

tasks.shadowJar {
    manifest {
        attributes["Entry-Points"] = "com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl com.netcracker.profiler.agent.plugins.ProfilerTransformerPluginImpl com.netcracker.profiler.agent.plugins.DumperPluginImpl"
    }
}

val extraMavenPublications by configurations.getting

// (artifacts) {
//    extraMavenPublications(tasks.shadowJar) {
//        classifier = ""
//    }
// }
