plugins {
    id("build-logic.kotlin-dsl-gradle-plugin")
}

dependencies {
    constraints {
        api("org.eclipse.jgit:org.eclipse.jgit:7.8.0.202609011348-r")
    }
    implementation(project(":basics"))
    implementation(project(":build-parameters"))
    implementation(project(":verification"))
    api("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:2.3.21")
    api("org.jetbrains.kotlin.kapt:org.jetbrains.kotlin.kapt.gradle.plugin:2.4.20")
    implementation("com.github.vlsi.crlf:com.github.vlsi.crlf.gradle.plugin:4.0.0")
    implementation("com.github.vlsi.gradle-extensions:com.github.vlsi.gradle-extensions.gradle.plugin:4.0.0")
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin")
    implementation("com.github.autostyle:com.github.autostyle.gradle.plugin:4.0.1")
    implementation("com.github.vlsi.jandex:com.github.vlsi.jandex.gradle.plugin:4.0.0")
}
