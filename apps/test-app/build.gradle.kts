plugins {
    id("build-logic.java-published-library")
}

dependencies {
    compileOnly(projects.boot)
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.netcracker.profilerTest.testapp.Main"
    }
}
