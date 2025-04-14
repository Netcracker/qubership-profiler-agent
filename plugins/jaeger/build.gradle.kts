plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    implementation("io.opentracing:opentracing-api:0.32.0")
    implementation("io.jaegertracing:jaeger-core:1.0.0")
}
