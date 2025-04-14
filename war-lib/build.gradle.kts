import com.github.jengelman.gradle.plugins.shadow.transformers.PropertiesFileTransformer

plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
    id("com.gradleup.shadow")
}

dependencies {
    implementation(projects.dumper)
    implementation(projects.common)
    implementation(projects.web)
    implementation(projects.cli)
    implementation("org.slf4j:slf4j-api")
    implementation("com.github.ziplet:ziplet")
    implementation("stax:stax-api")
}

tasks.shadowJar {
    mergeServiceFiles()
    append("META-INF/spring.handlers")
    append("META-INF/spring.schemas")
    append("META-INF/spring.tooling")
    append("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")
    transform(PropertiesFileTransformer::class.java) {
        paths.add("META-INF/spring.factories")
        mergeStrategy = "append"
    }
    relocate("ch.qos.logback", "org.qubership.profiler.shaded.ch.qos.logback")
    relocate("com.fasterxml", "org.qubership.profiler.shaded.com.fasterxml")
    relocate("com.github.ziplet", "org.qubership.profiler.shaded.com.github.ziplet")
    relocate("com.google", "org.qubership.profiler.shaded.com.google")
    relocate("com.graphbuilder", "org.qubership.profiler.shaded.com.graphbuilder")
    relocate("com.microsoft.schemas", "org.qubership.profiler.shaded.com.microsoft.schemas")
    relocate("gnu.trove", "org.qubership.profiler.shaded.gnu.trove")
    relocate("net.sourceforge.argparse4j", "org.qubership.profiler.shaded.net.sourceforge.argparse4j")
    relocate("org.HdrHistogram", "org.qubership.profiler.shaded.org.HdrHistogram")
    relocate("org.aopalliance", "org.qubership.profiler.shaded.org.aopalliance")
    relocate("org.apache", "org.qubership.profiler.shaded.org.apache")
    relocate("org.etsi.uri", "org.qubership.profiler.shaded.org.etsi.uri")
    relocate("org.objectweb", "org.qubership.profiler.shaded.org.objectweb")
    relocate("org.openjdk.jmc", "org.qubership.profiler.shaded.org.openjdk.jmc")
    relocate("org.openxmlformats", "org.qubership.profiler.shaded.org.openxmlformats")
    relocate("org.slf4j", "org.qubership.profiler.shaded.org.slf4j")
    relocate("org.springframework", "org.qubership.profiler.shaded.org.springframework")
    relocate("org.w3.x2000", "org.qubership.profiler.shaded.org.w3.x2000")
    relocate("schemaorg_apache_xmlbeans", "org.qubership.profiler.shaded.schemaorg_apache_xmlbeans")
}
