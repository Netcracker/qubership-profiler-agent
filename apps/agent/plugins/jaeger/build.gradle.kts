plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    injectorImplementation("io.opentracing:opentracing-api:0.32.0")
}

// 1.0.0 is the first 1.x release, and bom-testing tracks the latest. jaeger-core brings in
// opentracing-util, which holds the scope manager the plugin instruments.
instrumentationTestLibraries(
    "io.jaegertracing:jaeger-core" to versions("1.0.0"),
)
