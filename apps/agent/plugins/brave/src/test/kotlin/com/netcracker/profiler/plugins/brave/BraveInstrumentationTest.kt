package com.netcracker.profiler.plugins.brave

import com.netcracker.profiler.testkit.instrumentation.PluginInstrumentationTest

/**
 * Brave moved `newScope` from `CurrentTraceContext.Default` to `ThreadLocalCurrentTraceContext` in
 * 5.2.0, so this plugin carries a rule per class and the versions in `build.gradle.kts` cover both.
 */
class BraveInstrumentationTest : PluginInstrumentationTest()
