plugins {
    id("build-logic.build-params")
    id("build-logic.repositories")
    id("build-logic.root-build")
    // IDE configuration
    id("com.github.vlsi.ide")
    id("com.github.vlsi.gradle-extensions")
    id("jacoco")
    kotlin("jvm") apply false
}

ide {
    ideaInstructionsUri =
        uri("https://github.com/Netcracker/qubership-profiler-agent")
    doNotDetectFrameworks("android", "jruby")
}

val String.v: String get() = rootProject.extra["$this.version"] as String

val buildVersion = "profiler".v + (if (buildParameters.release) "" else "-SNAPSHOT")

println("Building Profiler $buildVersion")

jacoco {
    toolVersion = "0.8.14"
    providers.gradleProperty("jacoco.version")
        .takeIf { it.isPresent }
        ?.let { toolVersion = it.get() }
}

val jacocoReport by tasks.registering(JacocoReport::class) {
    group = "Coverage reports"
    description = "Generates an aggregate report from all subprojects"
}

allprojects {
    group = "org.qubership.profiler"
    version = buildVersion
}

val parameters by tasks.registering {
    group = HelpTasksPlugin.HELP_GROUP
    description = "Displays build parameters (i.e. -P flags) that can be used to customize the build"
    dependsOn(gradle.includedBuild("build-logic").task(":build-parameters:parameters"))
}

dependencies {
    nmcpAggregation(projects.agent)
    nmcpAggregation(projects.boot)
    nmcpAggregation(projects.common)
    nmcpAggregation(projects.cli)
    nmcpAggregation(projects.diagtools)
    nmcpAggregation(projects.dumper)
    nmcpAggregation(projects.installer)
    nmcpAggregation(projects.instrumenter)
    nmcpAggregation(projects.parsers)
    nmcpAggregation(projects.pluginGenerator)
    nmcpAggregation(projects.pluginRuntime)
    nmcpAggregation(projects.protoDefinition)
    nmcpAggregation(projects.runtime)
    nmcpAggregation(projects.warLib)
    nmcpAggregation(projects.web)
    nmcpAggregation(projects.plugins.activemq)
    nmcpAggregation(projects.plugins.ant)
    nmcpAggregation(projects.plugins.ant1102)
    nmcpAggregation(projects.plugins.apacheFelix)
    nmcpAggregation(projects.plugins.apacheHttpclient)
    nmcpAggregation(projects.plugins.brave)
    nmcpAggregation(projects.plugins.cassandra)
    nmcpAggregation(projects.plugins.cassandra4)
    nmcpAggregation(projects.plugins.couchbase)
    nmcpAggregation(projects.plugins.elasticsearch)
    nmcpAggregation(projects.plugins.equinox)
    nmcpAggregation(projects.plugins.hornetq)
    nmcpAggregation(projects.plugins.http)
    nmcpAggregation(projects.plugins.jackson)
    nmcpAggregation(projects.plugins.jaeger)
    nmcpAggregation(projects.plugins.javaHttpClient)
    nmcpAggregation(projects.plugins.jettyHttp)
    nmcpAggregation(projects.plugins.jetty10Http)
    nmcpAggregation(projects.plugins.liferay)
    nmcpAggregation(projects.plugins.log4jEnhancer)
    nmcpAggregation(projects.plugins.mysql)
    nmcpAggregation(projects.plugins.ocelot)
    nmcpAggregation(projects.plugins.postgresql)
    nmcpAggregation(projects.plugins.quartz)
    nmcpAggregation(projects.plugins.rabbitmq)
    nmcpAggregation(projects.plugins.rhino)
    nmcpAggregation(projects.plugins.spring)
    nmcpAggregation(projects.plugins.springrest)
    nmcpAggregation(projects.plugins.tomcatHttp)
    nmcpAggregation(projects.plugins.tomcat10Http)
    nmcpAggregation(projects.plugins.undertowHttp)
    nmcpAggregation(projects.plugins.undertow23Http)
    nmcpAggregation(projects.plugins.vertx)
}
