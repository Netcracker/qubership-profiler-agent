plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    implementation("com.liferay.portal:portal-service:6.2.5")
    implementation("javax.portlet:portlet-api:2.0")
    implementation("javax:javaee-api")
}
