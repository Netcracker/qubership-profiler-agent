plugins {
    id("build-logic.java-published-library")
    id("build-logic.test-testng")
}

dependencies {
    implementation(projects.boot)
    implementation(projects.common)
    implementation(projects.parsers)
    api("javax:javaee-api")
    implementation("ch.qos.logback:logback-access")
    implementation("ch.qos.logback:logback-classic")
    implementation("ch.qos.logback:logback-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("commons-lang:commons-lang")
    implementation("org.openjdk.jmc:common")
    implementation("org.openjdk.jmc:flightrecorder")
    implementation("org.springframework.boot:spring-boot")
    implementation("org.springframework.boot:spring-boot-autoconfigure")
}
