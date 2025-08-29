plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    injectorImplementation("com.liferay.portal:portal-service:6.0.6")
    injectorImplementation("javax.portlet:portlet-api:2.0")
    injectorImplementation("javax:javaee-api")
}
