plugins {
    id("build-logic.java-published-library")
}

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T : Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val installerZipElements by configurations.dependencyScope("installerZipElements") {
}

val installerZipFiles = configurations.resolvable("installerZipFiles") {
    isTransitive = false
    extendsFrom(installerZipElements)
    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, Category.LIBRARY)
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.JAR)
        attribute(Usage.USAGE_ATTRIBUTE, Usage.JAVA_RUNTIME)
        attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.EXTERNAL)
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, buildParameters.targetJavaVersion)
    }
}

dependencies {
    installerZipElements(projects.boot)
    installerZipElements(projects.agent)
    installerZipElements(projects.runtime) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, Bundling.SHADOWED)
        }
    }
    installerZipElements(projects.plugins.activemq)
    installerZipElements(projects.plugins.ant)
    installerZipElements(projects.plugins.ant1102)
    installerZipElements(projects.plugins.apacheFelix)
    installerZipElements(projects.plugins.apacheHttpclient)
    installerZipElements(projects.plugins.brave)
    installerZipElements(projects.plugins.cassandra)
    installerZipElements(projects.plugins.cassandra4)
    installerZipElements(projects.plugins.couchbase)
    installerZipElements(projects.plugins.elasticsearch)
    installerZipElements(projects.plugins.equinox)
    installerZipElements(projects.plugins.hornetq)
    installerZipElements(projects.plugins.http)
    installerZipElements(projects.plugins.jackson)
    installerZipElements(projects.plugins.jaeger)
    installerZipElements(projects.plugins.javaHttpClient)
    installerZipElements(projects.plugins.jettyHttp)
    installerZipElements(projects.plugins.jetty10Http)
    installerZipElements(projects.plugins.liferay)
    installerZipElements(projects.plugins.log4jEnhancer)
    installerZipElements(projects.plugins.mysql)
    installerZipElements(projects.plugins.ocelot)
    installerZipElements(projects.plugins.postgresql)
    installerZipElements(projects.plugins.quartz)
    installerZipElements(projects.plugins.rabbitmq)
    installerZipElements(projects.plugins.rhino)
    installerZipElements(projects.plugins.spring)
    installerZipElements(projects.plugins.springrest)
    installerZipElements(projects.plugins.tomcatHttp)
    installerZipElements(projects.plugins.tomcat10Http)
    installerZipElements(projects.plugins.tracing)
    installerZipElements(projects.plugins.undertowHttp)
    installerZipElements(projects.plugins.undertow23Http)
    installerZipElements(projects.plugins.vertx)
}

val installerZip by tasks.registering(Zip::class) {
    from(sourceSets.main.get().resources)
    into("lib") {
        from(installerZipFiles)
    }
    // TODO: do we really need extracting the config files? Could the agent look into the jars?
    into(".") {
        includeEmptyDirs = false
        from(
            installerZipFiles.get().elements.map { jars ->
                jars.map { jar ->
                    zipTree(jar)
                        .matching {
                            include("config/**/*.xml")
                        }
                }
            }
        )
    }
}
