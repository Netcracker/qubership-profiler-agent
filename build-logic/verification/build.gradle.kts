plugins {
    id("build-logic.kotlin-dsl-gradle-plugin")
}

dependencies {
    constraints {
        api("org.eclipse.jgit:org.eclipse.jgit:7.4.0.202509020913-r")
    }
    implementation(project(":basics"))
    implementation(project(":build-parameters"))
    implementation("com.github.autostyle:com.github.autostyle.gradle.plugin:4.0")
    implementation("com.github.vlsi.gradle-extensions:com.github.vlsi.gradle-extensions.gradle.plugin:2.0.0")
    implementation("de.thetaphi.forbiddenapis:de.thetaphi.forbiddenapis.gradle.plugin:3.9")
    implementation("net.ltgt.errorprone:net.ltgt.errorprone.gradle.plugin:4.3.0")
    implementation("org.checkerframework:org.checkerframework.gradle.plugin:0.6.60")
}
