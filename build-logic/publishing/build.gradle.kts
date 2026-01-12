plugins {
    id("build-logic.kotlin-dsl-gradle-plugin")
}

dependencies {
    implementation(project(":basics"))
    implementation(project(":jvm"))
    implementation(project(":build-parameters"))
    implementation("com.github.vlsi.gradle-extensions:com.github.vlsi.gradle-extensions.gradle.plugin:3.0.1")
    implementation("com.gradleup.nmcp:com.gradleup.nmcp.gradle.plugin:1.4.3")
    implementation("com.gradleup.shadow:com.gradleup.shadow.gradle.plugin:9.3.1")
    implementation("org.apache.ant:ant:1.10.15")
}
