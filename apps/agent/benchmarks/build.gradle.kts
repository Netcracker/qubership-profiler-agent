plugins {
    id("build-logic.kotlin")
    id("me.champeau.jmh")
}

dependencies {
    jmhImplementation(projects.boot)
}

tasks.withType<JavaExec>().configureEach {
    // Execution of .main methods from IDEA should re-generate benchmark classes if required
    dependsOn("jmhCompileGeneratedClasses")
    doFirst {
        // At best jmh plugin should add the generated directories to the Gradle model, however,
        // currently it builds the jar only :-/
        // IntelliJ IDEA "execute main method" adds a JavaExec task, so we configure it
        classpath(layout.buildDirectory.dir("jmh-generated-classes"))
        classpath(layout.buildDirectory.dir("jmh-generated-resources"))
    }
}
