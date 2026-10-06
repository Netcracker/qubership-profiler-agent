plugins {
    id("build-logic.profiler-published-plugin")
    id("build-logic.test-junit5")
    id("build-logic.kotlin")
}

// ChannelN.basicPublish gained a seven-argument ByteBuffer overload in amqp-client 5.31.0, and the
// profiler binds an injected method by the exact descriptor of the call site, so each published body
// type needs its own basicPublish$profiler. 5.30.0 and 5.31.0 are the two sides of that boundary.
//
// spring-amqp 4.0 changed MessagingMessageListenerAdapter.invokeHandler to take the channel first
// and the AMQP messages last as a varargs array, so rabbitmq.xml carries a rule per shape. 2.4.17
// and 3.2.6 are the old shape, and the version bom-testing tracks is the new one.
instrumentationTestLibraries(
    "com.rabbitmq:amqp-client" to versions("5.30.0", "5.31.0"),
    "org.springframework.amqp:spring-rabbit" to versions("2.4.17", "3.2.6"),
)
