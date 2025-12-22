plugins {
    id("build-logic.java-published-library")
    id("com.google.osdetector")
}

class TargetPlatform(val os: OperatingSystemFamily, val architecture: MachineArchitecture) {
    val id: String get() = "${os.name}_${architecture.name}"
}

fun targetPlatform(os: String, architecture: String): TargetPlatform =
    TargetPlatform(objects.named(os), objects.named(architecture))

val platforms = mapOf(
    targetPlatform(OperatingSystemFamily.WINDOWS, MachineArchitecture.X86_64) to "windows-amd64",
    targetPlatform(OperatingSystemFamily.LINUX, MachineArchitecture.X86_64) to "linux-amd64",
    targetPlatform(OperatingSystemFamily.LINUX, MachineArchitecture.ARM64) to "linux-arm64",
) + when {
    // Currently, diagtools depends on jattach that uses native code
    // which is hard to build without mac toolchains. So we skip generating and publishing mac binaries
    // when building on linux machines (e.g. GitHub Actions)
    osdetector.os != "osx" -> emptyMap()
    else ->
        mapOf(
            targetPlatform(OperatingSystemFamily.MACOS, MachineArchitecture.X86_64) to "darwin-amd64",
            targetPlatform(OperatingSystemFamily.MACOS, MachineArchitecture.ARM64) to "darwin-arm64",
        )
}

val binaries by configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.NATIVE_RUNTIME))
    }
    outgoing {
        variants {
            for ((platform, platformId) in platforms) {
                create(platformId) {
                    attributes {
                        attribute(OperatingSystemFamily.OPERATING_SYSTEM_ATTRIBUTE, platform.os)
                        attribute(MachineArchitecture.ARCHITECTURE_ATTRIBUTE, platform.architecture)
                    }
                }
            }
        }
    }
}

(components["java"] as AdhocComponentWithVariants).addVariantsFromConfiguration(binaries) {
}

val buildAll by tasks.registering(Exec::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Build diagtool binaries"
    commandLine = listOf("make", "build-all")
}

for ((platform, platformId) in platforms) {
    val archiveFormat = if (platform.os.name == OperatingSystemFamily.WINDOWS) Zip::class else Tar::class
    val packageTask = tasks.register("package_$platformId", archiveFormat) {
        dependsOn(buildAll)
        group = LifecycleBasePlugin.BUILD_GROUP
        description = "Package diagtools distribution for $platformId"
        archiveClassifier.set(platformId)
        if (this is Tar) {
            compression = Compression.GZIP
            archiveExtension = "tar.gz"
        }
        into("diagtools-${project.version}-$platformId") {
            from("README.md")
            from(rootProject.layout.projectDirectory.file("LICENSE"))
            from("scripts") {
                filePermissions {
                    unix("rwxr-xr-x")
                }
            }
            from(layout.buildDirectory.file("diagtools-$platformId${if (platform.os.isWindows) ".exe" else ""}")) {
                rename {
                    "diagtools${if (platform.os.isWindows) ".exe" else ""}"
                }
                filePermissions {
                    unix("rwxr-xr-x")
                }
            }
        }
    }
    binaries.outgoing.variants {
        named(platformId) {
            artifact(packageTask)
        }
    }
}
