plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    injectorImplementation("io.opentracing:opentracing-api:0.32.0")
    injectorImplementation("io.zipkin.brave:brave:4.0.0")
}

// 4.0.0 is the release the injector compiles against, and it has no current-context class at all.
// CurrentTraceContext.Default declares newScope itself up to 5.1.0 and inherits it from
// ThreadLocalCurrentTraceContext from 5.2.0 on, so brave.xml carries a rule for each class, and
// 5.1.0 and 5.2.0 are the two sides of that boundary.
instrumentationTestLibraries(
    "io.zipkin.brave:brave" to versions("4.0.0", "5.1.0", "5.2.0"),
)
