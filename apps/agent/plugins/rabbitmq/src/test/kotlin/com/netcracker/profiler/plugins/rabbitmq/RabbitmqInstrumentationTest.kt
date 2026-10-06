package com.netcracker.profiler.plugins.rabbitmq

import com.netcracker.profiler.testkit.instrumentation.LibraryUnderTest
import com.netcracker.profiler.testkit.instrumentation.PluginInstrumentation
import com.netcracker.profiler.testkit.instrumentation.PluginInstrumentationTest

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

/**
 * `ChannelN.basicPublish` gained a `ByteBuffer` overload in amqp-client 5.31.0 and
 * `MessagingMessageListenerAdapter.invokeHandler` changed shape in spring-amqp 4.0, so this plugin
 * carries a rule per signature and the versions in `build.gradle.kts` cover both sides of each
 * change.
 */
class RabbitmqInstrumentationTest : PluginInstrumentationTest() {
    /**
     * Each rule here claims the `basicPublish` overload that performs the publish on the client at
     * hand, so no publish may pass through two instrumented frames.
     */
    @ParameterizedTest
    @MethodSource("libraries")
    fun `no publish is instrumented twice`(library: LibraryUnderTest) =
        PluginInstrumentation.assertNoOperationIsInstrumentedTwice(library)
}
