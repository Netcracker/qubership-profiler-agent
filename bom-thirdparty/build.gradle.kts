plugins {
    id("build-logic.java-published-platform")
}

description = "A collection of versions of third-party libraries used by Qubership Profiler Agent"

javaPlatform {
    allowDependencies()
}

dependencies {
    api(platform("org.ow2.asm:asm-bom:9.5"))
    api(platform("org.springframework.boot:spring-boot-dependencies:1.5.1.RELEASE"))
    constraints {
        api("backport-util-concurrent:backport-util-concurrent:3.1")
        api("ch.qos.logback:logback-access:1.2.9")
        api("ch.qos.logback:logback-classic:1.2.9")
        api("ch.qos.logback:logback-core:1.2.9")
        api("com.github.ziplet:ziplet:2.1.2")
        api("commons-io:commons-io:2.11.0")
        api("commons-lang:commons-lang:2.6")
        api("javax:javaee-api:6.0")
        api("net.sf.trove4j:trove4j:2.1.0")
        api("net.sourceforge.argparse4j:argparse4j:0.4.3")
        api("org.apache.httpcomponents:httpcore:4.4.16")
        api("org.apache.tomcat.embed:tomcat-embed-core:8.5.100")
        api("org.apache.tomcat.embed:tomcat-embed-logging-juli:8.5.2")
        api("org.hdrhistogram:HdrHistogram:2.1.11")
        api("org.openjdk.jmc:common:8.0.1")
        api("org.openjdk.jmc:flightrecorder:8.0.1")
        api("org.slf4j:slf4j-api:1.7.36")
        api("stax:stax-api:1.0.1")
    }
}
