plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("com.fasterxml.jackson:jackson-bom:2.20.0"))
    api(platform("org.ow2.asm:asm-bom:9.8"))
    api(platform("org.springframework.boot:spring-boot-dependencies:2.7.18"))
    constraints {
        api("backport-util-concurrent:backport-util-concurrent:3.1")
        api("ch.qos.logback:logback-classic:1.5.18")
        api("ch.qos.logback:logback-core:1.5.18")
        api("com.github.ajalt.clikt:clikt:5.0.3")
        api("com.github.ziplet:ziplet:2.4.1")
        api("commons-io:commons-io:2.20.0")
        api("commons-lang:commons-lang:2.6")
        api("javax:javaee-api:6.0")
        api("net.sf.trove4j:trove4j:3.0.3")
        api("net.sourceforge.argparse4j:argparse4j:0.9.0")
        api("org.apache.httpcomponents:httpcore:4.4.16")
        api("org.apache.tomcat.embed:tomcat-embed-core:9.0.111")
        api("org.apache.tomcat.embed:tomcat-embed-logging-juli:8.5.2")
        api("org.hdrhistogram:HdrHistogram:2.2.2")
        api("org.openjdk.jmc:common:8.3.1")
        api("org.openjdk.jmc:flightrecorder:8.3.1")
        api("org.slf4j:slf4j-api:2.0.17")
        api("stax:stax-api:1.0.1")
    }
}
