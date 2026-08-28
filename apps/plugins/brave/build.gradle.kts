plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    injectorImplementation("io.opentracing:opentracing-api:0.32.0")
    injectorImplementation("io.zipkin.brave:brave:4.0.0")
}
