plugins {
    id("build-logic.kotlin-dsl-gradle-plugin")
}

dependencies {
    constraints {
        api("org.eclipse.jgit:org.eclipse.jgit:7.4.0.202509020913-r")
    }
    implementation(project(":basics"))
    implementation(project(":build-parameters"))
    implementation(project(":verification"))
    api("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:2.2.20")
    api("org.jetbrains.kotlin.kapt:org.jetbrains.kotlin.kapt.gradle.plugin:2.2.20")
    implementation("com.github.vlsi.crlf:com.github.vlsi.crlf.gradle.plugin:2.0.0")
    implementation("com.github.vlsi.gradle-extensions:com.github.vlsi.gradle-extensions.gradle.plugin:2.0.0")
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin")
    implementation("com.github.autostyle:com.github.autostyle.gradle.plugin:4.0")
    implementation("com.github.vlsi.jandex:com.github.vlsi.jandex.gradle.plugin:2.0.0")
}
