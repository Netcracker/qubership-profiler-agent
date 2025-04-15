plugins {
    // TODO: replace with war
    id("build-logic.java-published-library")
    id("war")
}

sourceSets {
    // Classes from src/main go to WEB-INF/classes, however we need to put
    // launcher classes to a top-level folder so the jar is runnable
    // So we create a separate source set for it
    create("launcher")
}

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val jsResourcesElements = configurations.dependencyScope("jsResourcesElements") {
}

val jsResources = configurations.resolvable("jsResources") {
    extendsFrom(jsResourcesElements.get())
    attributes {
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.RESOURCES)
        attribute(Attribute.of("org.qubership.js.optimization", String::class.java), "prod")
    }
}

val jsSinglePageResources = configurations.resolvable("jsSinglePageResources") {
    extendsFrom(jsResourcesElements.get())
    attributes {
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.RESOURCES)
        attribute(Attribute.of("org.qubership.js.optimization", String::class.java), "single-page")
    }
}

dependencies {
    implementation(projects.warLib) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.SHADOWED)
        }
    }
    "launcherImplementation"(platform(projects.bomThirdparty))
    "launcherImplementation"("org.apache.tomcat.embed:tomcat-embed-core")
    jsResourcesElements(projects.profilerUi)
}

tasks.war {
    manifest {
        attributes["Bundle-Version"] = "${project.version}"
        attributes["Bundle-Category"] = "Qubership Tools"
        attributes["Bundle-Name"] = "Qubership Profiler"
        attributes["Bundle-SymbolicName"] = "org.qubership.profiler"
        attributes["Bundle-ManifestVersion"] = "2"
        attributes["Import-Package"] = "javax.servlet,javax.servlet.http,org.w3c.dom,javax.naming,org.xml.sax,javax.xml.parsers,javax.xml.transform,javax.xml.transform.dom,javax.xml.transform.sax,javax.xml.transform.stax,javax.xml.transform.stream"
        attributes["Bundle-ClassPath"] = ".,WEB-INF/lib/war-lib-${project.version}.jar"
        attributes["Main-Class"] = "org.qubership.profiler.WARLauncher"
        attributes["Web-ContextPath"] = "profiler"
    }
    from(sourceSets["launcher"].output)
    from(configurations["launcherRuntimeClasspath"].elements.map { jars ->
        jars.map { zipTree(it) }
    }) {
        duplicatesStrategy = DuplicatesStrategy.WARN
        // Tomcat jars are signed, and we can't reuse the signatures
        exclude("**/*.SF")
        exclude("**/*.RSA")
        exclude("**/*.DSA")
    }
    from(jsResources)
    into("single-page") {
        from(jsSinglePageResources)
    }
}

val runWar by tasks.registering(JavaExec::class) {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Runs this project as a WAR application"
    classpath = files(tasks.war)
    // Tomcat needs this when running with Java 9+
    jvmArgs("--add-opens=java.base/java.io=ALL-UNNAMED")
    jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
    jvmArgs("--add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED")
}
