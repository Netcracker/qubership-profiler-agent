plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    injectorImplementation("io.opentracing:opentracing-api:0.33.0")
    injectorImplementation("io.jaegertracing:jaeger-core:1.8.1")
}
