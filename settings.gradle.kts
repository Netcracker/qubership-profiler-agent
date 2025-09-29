pluginManagement {
    plugins {
        id("com.github.vlsi.crlf") version "2.0.0"
        id("com.github.vlsi.gettext") version "2.0.0"
        id("com.github.vlsi.gradle-extensions") version "2.0.0"
        id("com.github.vlsi.ide") version "2.0.0"
        id("com.gradleup.shadow") version "8.3.9"
        id("com.github.node-gradle.node") version "7.1.0"
        kotlin("jvm") version "2.2.20"
        kotlin("kapt") version "2.2.20"
    }
}

plugins {
}

if (JavaVersion.current() < JavaVersion.VERSION_17) {
    throw UnsupportedOperationException("Please use Java 17 or 21 for launching Gradle when building profiler, the current Java is ${JavaVersion.current().majorVersion}")
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// This is the name of a current project
// Note: it cannot be inferred from the directory name as developer might clone the project to folder with any name
rootProject.name = "qubership-profiler"

includeBuild("build-logic")

// Renovate treats names as dependency coordinates when vararg include(...) is used, so we have separate include calls here
include("agent")
include("bom-testing")
include("bom-thirdparty")
include("boot")
include("cli")
include("common")
include("dumper")
include("jcstress-jupiter-engine")
include("installer")
include("installer-zip-test")
include("instrumenter")
include("it-test")
include("parsers")
include("plugin-generator")
include("plugin-runtime")
include("profiler")
include("profiler-ui")
include("proto-definition")
include("runtime")
include("test-config")
include("war-lib")
include("web")

include("plugins:activemq")
include("plugins:ant")
include("plugins:ant_1102")
include("plugins:apache_felix")
include("plugins:apache_httpclient")
include("plugins:brave")
include("plugins:cassandra")
include("plugins:cassandra4")
include("plugins:couchbase")
include("plugins:elasticsearch")
include("plugins:equinox")
include("plugins:hornetq")
include("plugins:http")
include("plugins:jackson")
include("plugins:jaeger")
include("plugins:java_http_client")
include("plugins:jetty10_http")
include("plugins:jetty_http")
include("plugins:liferay")
include("plugins:log4j_enhancer")
include("plugins:mysql")
include("plugins:ocelot")
include("plugins:postgresql")
include("plugins:quartz")
include("plugins:rabbitmq")
include("plugins:rhino")
include("plugins:spring")
include("plugins:springrest")
include("plugins:test")
include("plugins:tomcat10_http")
include("plugins:tomcat_http")
include("plugins:undertow23_http")
include("plugins:undertow_http")
include("plugins:vertx")


// See https://github.com/gradle/gradle/issues/1348#issuecomment-284758705 and
// https://github.com/gradle/gradle/issues/5321#issuecomment-387561204
// Gradle inherits Ant "default excludes", however we do want to archive those files
org.apache.tools.ant.DirectoryScanner.removeDefaultExclude("**/.gitattributes")
org.apache.tools.ant.DirectoryScanner.removeDefaultExclude("**/.gitignore")

fun property(name: String) =
    when (extra.has(name)) {
        true -> extra.get(name) as? String
        else -> null
    }
