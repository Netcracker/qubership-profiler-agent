package com.netcracker.profiler.testkit.instrumentation

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * Checks a plugin's injectors against the real libraries it instruments, at the versions
 * `instrumentationTestLibraries` lists in the plugin's `build.gradle.kts`.
 *
 * A plugin's own test declares nothing but the class: [PluginInstrumentation] reads the plugin's
 * configuration, asks the transformer about every class in each library, and checks the ones the
 * configuration selects, so a rule added to that configuration is covered without an edit here.
 *
 * ```kotlin
 * class HttpInstrumentationTest : PluginInstrumentationTest()
 * ```
 *
 * [PluginInstrumentation.assertNoOperationIsInstrumentedTwice] is not among these, because a
 * configuration may nest its rules on purpose. A plugin whose rules claim one method per operation
 * adds it as a test of its own.
 */
abstract class PluginInstrumentationTest {
    @ParameterizedTest
    @MethodSource("libraries")
    fun `every reference the instrumentation adds is declared`(library: LibraryUnderTest) =
        PluginInstrumentation.assertAddedReferencesResolve(library)

    @ParameterizedTest
    @MethodSource("libraries")
    fun `every instrumented class passes bytecode verification`(library: LibraryUnderTest) =
        PluginInstrumentation.assertInstrumentedClassesVerify(library)

    @ParameterizedTest
    @MethodSource("libraries")
    fun `every method the configuration selects is instrumented`(library: LibraryUnderTest) =
        PluginInstrumentation.assertSelectedMethodsAreInstrumented(library)

    companion object {
        @JvmStatic
        fun libraries() = PluginInstrumentation.librariesUnderTest()
    }
}
