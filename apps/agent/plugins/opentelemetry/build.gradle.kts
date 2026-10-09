plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

dependencies {
    // The oldest API the plugin supports, so an injector cannot call a method a later release added.
    injectorImplementation("io.opentelemetry:opentelemetry-api:1.0.0")
}

// The SDK span class is RecordEventsReadableSpan up to 1.12 and SdkSpan from 1.13.0, and
// opentelemetry.xml carries a rule for each name. 1.0.0 and 1.13.0 are the first release of each, and
// bom-testing tracks the latest. opentelemetry-sdk-trace brings in opentelemetry-context, which holds
// the third class the plugin instruments.
instrumentationTestLibraries(
    "io.opentelemetry:opentelemetry-sdk-trace" to versions("1.0.0", "1.13.0"),
)
