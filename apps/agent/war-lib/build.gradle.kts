plugins {
    id("build-logic.java-published-library")
    id("com.gradleup.shadow")
}

dependencies {
    implementation(projects.dumper)
    implementation(projects.common)
    implementation(projects.web)
    implementation(projects.cli)
    implementation("org.slf4j:slf4j-api")
    implementation("stax:stax-api")
}

tasks.shadowJar {
    mergeServiceFiles()
    relocate("ch.qos.logback", "com.netcracker.profiler.shaded.ch.qos.logback")
    relocate("com.fasterxml", "com.netcracker.profiler.shaded.com.fasterxml")
    relocate("com.google", "com.netcracker.profiler.shaded.com.google")
    relocate("com.graphbuilder", "com.netcracker.profiler.shaded.com.graphbuilder")
    relocate("com.microsoft.schemas", "com.netcracker.profiler.shaded.com.microsoft.schemas")
    relocate("gnu.trove", "com.netcracker.profiler.shaded.gnu.trove")
    relocate("net.sourceforge.argparse4j", "com.netcracker.profiler.shaded.net.sourceforge.argparse4j")
    relocate("org.HdrHistogram", "com.netcracker.profiler.shaded.org.HdrHistogram")
    relocate("org.aopalliance", "com.netcracker.profiler.shaded.org.aopalliance")
    relocate("org.apache", "com.netcracker.profiler.shaded.org.apache")
    relocate("org.etsi.uri", "com.netcracker.profiler.shaded.org.etsi.uri")
    relocate("org.objectweb", "com.netcracker.profiler.shaded.org.objectweb")
    relocate("org.openjdk.jmc", "com.netcracker.profiler.shaded.org.openjdk.jmc")
    relocate("org.openxmlformats", "com.netcracker.profiler.shaded.org.openxmlformats")
    relocate("org.slf4j", "com.netcracker.profiler.shaded.org.slf4j")
    relocate("org.w3.x2000", "com.netcracker.profiler.shaded.org.w3.x2000")
    relocate("schemaorg_apache_xmlbeans", "com.netcracker.profiler.shaded.schemaorg_apache_xmlbeans")
}
