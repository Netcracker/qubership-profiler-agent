import com.github.vlsi.gradle.dsl.configureEach

plugins {
    id("java-library")
    id("build-logic.build-params")
}

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val jmockitAgent = configurations.dependencyScope("jmockitAgent")
val jmockitAgentClasspath = configurations.resolvable("jmockitAgentClasspath") {
    extendsFrom(jmockitAgent.get())

    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.JAR)
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.EXTERNAL)
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
}

dependencies {
    findProject(":bom-testing")?.let {
        jmockitAgent(platform(it))
    }
    jmockitAgent("org.jmockit:jmockit")
    testImplementation("org.jmockit:jmockit")
    testImplementation("org.jmockit:jmockit-coverage")
}

tasks.configureEach<Test> {
    systemProperty("coverage-redundancy", "true")

    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-javaagent:${jmockitAgentClasspath.get().incoming.artifactView {
                componentFilter { it is ModuleComponentIdentifier && it.module == "jmockit" }
            }.files.singleFile}"
        )
    })
}
