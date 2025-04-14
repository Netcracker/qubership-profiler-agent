plugins {
    id("build-logic.profiler-published-plugin")
}

dependencies {
    implementation("io.opentracing:opentracing-api:0.32.0")
    implementation("io.zipkin.brave:brave:4.0.0")
}
