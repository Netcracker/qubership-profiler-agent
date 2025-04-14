import com.github.vlsi.gradle.dsl.configureEach
import com.github.vlsi.gradle.publishing.dsl.versionFromResolution

plugins {
    id("build-logic.build-params")
    id("build-logic.java-library")
    id("build-logic.reproducible-builds")
    id("build-logic.publish-to-central")
}

java {
    withSourcesJar()
    if (!buildParameters.skipJavadoc) {
        withJavadocJar()
    }
}

val extraMavenPublications by configurations.creating {
    isVisible = false
    isCanBeResolved = false
    isCanBeConsumed = false
}

val archivesName = "qubership-profiler-$name"
base.archivesName.set(archivesName)

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = archivesName

            // Gradle feature variants can't be mapped to Maven's pom
            suppressAllPomMetadataWarnings()

            // Use the resolved versions in pom.xml
            // Gradle might have different resolution rules, so we set the versions
            // that were used in Gradle build/test.
            versionFromResolution()
        }
        // artifacts.removeIf below resolves the configuration, so it causes the following warning:
        // Mutating a configuration after it has been resolved, consumed as a variant, or used for generating published metadata
        afterEvaluate {
            configureEach<MavenPublication> {
                // <editor-fold defaultstate="collapsed" desc="Override published artifacts (e.g. shaded instead of regular)">
                extraMavenPublications.outgoing.artifacts.apply {
                    val keys = mapTo(mutableSetOf()) {
                        it.classifier.orEmpty() to it.extension
                    }
                    artifacts.removeIf {
                        keys.contains(it.classifier.orEmpty() to it.extension)
                    }
                    forEach { artifact(it) }
                }
                // </editor-fold>
            }
        }
        configureEach<MavenPublication> {
            // Use the resolved versions in pom.xml
            // Gradle might have different resolution rules, so we set the versions
            // that were used in Gradle build/test.
            versionMapping {
                usage(Usage.JAVA_API) {
                    fromResolutionOf("runtimeClasspath")
                }
            }
        }
    }
}
