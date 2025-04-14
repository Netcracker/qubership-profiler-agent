import com.github.vlsi.gradle.dsl.configureEach
import org.gradle.api.publish.internal.PublicationInternal
import com.github.vlsi.gradle.publishing.dsl.simplifyXml
import java.util.Locale

plugins {
    id("build-logic.build-params")
    id("maven-publish")
    id("build-logic.publish-to-tmp-maven-repo")
    id("com.github.vlsi.gradle-extensions")
    id("com.gradleup.nmcp")
    id("signing")
}

val isSnapshot = project.version.toString().endsWith("-SNAPSHOT")

if (!isSnapshot) {
    signing {
        if (!isSnapshot) {
            sign(publishing.publications)
            if (!buildParameters.useInMemoryPgpKeys) {
                useGpgCmd()
            } else {
                val pgpPrivateKey = System.getenv("SIGNING_PGP_PRIVATE_KEY")
                val pgpPassphrase = System.getenv("SIGNING_PGP_PASSPHRASE")
                if (pgpPrivateKey.isNullOrBlank() || pgpPassphrase.isNullOrBlank()) {
                    throw IllegalArgumentException("GPP private key (SIGNING_PGP_PRIVATE_KEY) and passphrase (SIGNING_PGP_PASSPHRASE) must be set")
                }
                useInMemoryPgpKeys(
                    pgpPrivateKey,
                    pgpPassphrase
                )
            }
        }
    }
} else {
    publishing {
        repositories {
            maven {
                name = "centralSnapshots"
                url = uri("https://central.sonatype.com/repository/maven-snapshots")
                credentials(PasswordCredentials::class)
            }
        }
    }
}

publishing {
    publications.configureEach<MavenPublication> {
        // Use the resolved versions in pom.xml
        // Gradle might have different resolution rules, so we set the versions
        // that were used in Gradle build/test.
        versionMapping {
            usage(Usage.JAVA_RUNTIME) {
                fromResolutionResult()
            }
        }
        pom {
            simplifyXml()
            val capitalizedName = project.name
                .replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.getDefault()) else it.toString() }
            name.set(
                (project.findProperty("artifact.name") as? String) ?: "Qubership Profiler Agent $capitalizedName"
            )
            description.set(project.description ?: "Qubership Profiler Agent $capitalizedName")
            inceptionYear.set("2015")
            url.set("https://github.com/Netcracker/qubership-profiler-agent")
            licenses {
                license {
                    name.set("Apache-2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    comments.set("A business-friendly OSS license")
                    distribution.set("repo")
                }
            }
            organization {
                name.set("Netcracker Technology")
                url.set("https://www.netcracker.com")
            }
            developers {
                developer {
                    id.set("vlsi")
                    name.set("Vladimir Sitnikov")
                }
            }
            issueManagement {
                system.set("GitHub issues")
                url.set("https://github.com/Netcracker/qubership-profiler-agent/issues")
            }
            scm {
                connection.set("scm:git:https://github.com/Netcracker/qubership-profiler-agent.git")
                developerConnection.set("scm:git:https://github.com/Netcracker/qubership-profiler-agent.git")
                url.set("https://github.com/Netcracker/qubership-profiler-agent")
                tag.set("HEAD")
            }
        }
    }
}

val createReleaseBundle by tasks.registering(Sync::class) {
    description = "This task should be used by github actions to create release artifacts along with a slsa attestation"
    val releaseDir = layout.buildDirectory.dir("release")
    outputs.dir(releaseDir)

    into(releaseDir)
    rename("pom-default.xml", "${project.name}-${project.version}.pom")
    rename("module.json", "${project.name}-${project.version}.module")
}

publishing {
    publications.configureEach {
        (this as PublicationInternal<*>).allPublishableArtifacts {
            val publicationArtifact = this
            createReleaseBundle.configure {
                dependsOn(publicationArtifact)
                from(publicationArtifact.file)
            }
        }
    }
}

