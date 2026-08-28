plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    injectorImplementation("io.opentracing:opentracing-api:0.32.0")
    injectorImplementation("io.jaegertracing:jaeger-core:1.0.0")
}
