plugins {
    id("build-logic.java-library")
}

dependencies {
    api(platform(projects.bomTesting))
    api("org.junit.jupiter:junit-jupiter-engine")
    api("org.openjdk.jcstress:jcstress-core")
}
