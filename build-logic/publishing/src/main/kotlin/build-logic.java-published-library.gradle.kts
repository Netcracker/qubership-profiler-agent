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

val archivesName = "${isolated.rootProject.name}${path.replace(':', '-')}"
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
