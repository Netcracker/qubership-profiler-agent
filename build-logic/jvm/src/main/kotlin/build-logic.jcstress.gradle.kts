plugins {
    id("build-logic.java-library")
}

val jcstressSourceSet = sourceSets.create("jcstress") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

dependencies {
    jcstressSourceSet.annotationProcessorConfigurationName("org.openjdk.jcstress:jcstress-core")
}

//val jcstress by sourceSets.
//project.sourceSets

//val jcstress by sourceSets
