plugins {
    id("build-logic.build-params")
    id("build-logic.java-library")
}

tasks.jar {
    manifest {
        attributes["Entry-Points"] = "org.qubership.profiler.instrument.enhancement.EnhancerPlugin_${project.name}"
        attributes["Esc-Dependencies"] = "instrumenter"
    }
}

val injector by sourceSets.creating

tasks.checkstyleMain {
    // Exclude generated classes from the style check
    exclude("**/*Enhancers.java")
}

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T: Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val injectorImplementation by configurations.existing

val generateInjectorClasspath by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.JAR)
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.EXTERNAL)
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
}

dependencies {
    findProject(":boot")?.let {
        api(it)
        injectorImplementation(it)
    }
    findProject(":bom-thirdparty")?.let {
        injectorImplementation(platform(it))
    }
    findProject(":plugin-runtime")?.let {
        api(it)
    }
    findProject(":plugin-generator")?.let {
        generateInjectorClasspath(it)
    }
    findProject(":test-config")?.let {
        generateInjectorClasspath(it)
    }
    implementation("org.slf4j:slf4j-api")
    implementation("org.ow2.asm:asm-commons")
    implementation("org.ow2.asm:asm-util")
}

val generateInjectorDir = layout.buildDirectory.dir("generated/injector")

val generateInjector by tasks.registering(JavaExec::class) {
    dependsOn(injector.classesTaskName)
    outputs.dir(generateInjectorDir)
    buildParameters.buildJdk?.let {
        javaLauncher.convention(javaToolchains.launcherFor(it))
    }
    classpath(generateInjectorClasspath)
    mainClass = "org.qubership.profiler.tools.GenerateInjectorCommand"
    jvmArgs(
        "-Dorg.qubership.profiler.log.root_level=${
            when {
                logger.isTraceEnabled -> "trace"
                logger.isDebugEnabled -> "debug"
                logger.isInfoEnabled -> "info"
                logger.isWarnEnabled -> "warn"
                logger.isErrorEnabled -> "error"
                else -> "info"
            }
        }"
    )
    argumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "--output-file",
                generateInjectorDir.get()
                    .file("org/qubership/profiler/instrument/enhancement/EnhancerPlugin_${project.name}Enhancers.java")
                    .asFile.absolutePath
            )
        }
    )
    argumentProviders.add(
        CommandLineArgumentProvider {
            injector.output.classesDirs.flatMap { dir ->
                listOf("--input-class-directory", dir.absolutePath)
            }
        }
    )
}

sourceSets.main {
    java.srcDir(
        files(generateInjectorDir) {
            builtBy(generateInjector)
        }
    )
}
