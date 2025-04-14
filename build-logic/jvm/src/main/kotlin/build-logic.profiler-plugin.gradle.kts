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

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T: Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val generateInjectorClasspath by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    extendsFrom(configurations.api.get())
}

val injectorCompileClasspath by configurations.creating {
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
    findProject(":plugin-generator")?.let {
        implementation(project(":plugin-generator"))
        generateInjectorClasspath(project(":plugin-generator"))
        injectorCompileClasspath(project(":plugin-generator"))
    }
    findProject(":boot")?.let {
        implementation(project(":boot"))
    }
    findProject(":bom-thirdparty")?.let {
        injectorCompileClasspath(platform(project(":bom-thirdparty")))
    }
    implementation("org.slf4j:slf4j-api")
    injectorCompileClasspath("org.ow2.asm:asm-commons")
    injectorCompileClasspath("org.ow2.asm:asm-util")
}

val generateInjectorDir = layout.buildDirectory.dir("generated-injector")

val generateInjector by tasks.registering(JavaExec::class) {
    dependsOn(tasks.classes)
    outputs.dir(generateInjectorDir)
    buildParameters.buildJdk?.let {
        javaLauncher.convention(javaToolchains.launcherFor(it))
    }
    classpath(generateInjectorClasspath)
    mainClass = "org.qubership.profiler.tools.GenerateInjector"
    // root
    args(sourceSets.main.get().output.classesDirs)
    // destination
    args(layout.buildDirectory.file("generated-injector/org/qubership/profiler/instrument/enhancement/EnhancerPlugin_${project.name}Enhancers.java").get())
}

val generateInjectorClassesDir = layout.buildDirectory.dir("generated-injector-classes")

val compileInjector by tasks.registering(JavaCompile::class) {
    dependsOn(generateInjector)
    source(generateInjectorDir)
    classpath = injectorCompileClasspath
    destinationDirectory.set(generateInjectorClassesDir)
}

tasks.jar {
    from(compileInjector)
}
