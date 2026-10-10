package com.netcracker.profiler.plugins.opentelemetry

import com.netcracker.profiler.testkit.instrumentation.PluginInstrumentationTest

/**
 * The SDK renamed its span class in 1.13.0, so this plugin carries a rule per name and the versions
 * in `build.gradle.kts` cover both.
 */
class OpentelemetryInstrumentationTest : PluginInstrumentationTest()
