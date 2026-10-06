plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    injectorImplementation("com.liferay.portal:portal-service:6.0.6")
    injectorImplementation("javax.portlet:portlet-api:2.0")
    injectorImplementation("javax:javaee-api")
}

// portal-service 6.0.6 is the release the injector was written against, and Liferay has since moved
// the classes these rules name.
instrumentationTestLibraries(
    "com.liferay.portal:portal-service" to versions("6.0.6", trackLatest = false),
)
