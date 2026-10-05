package com.rabbitmq.client.impl;

import com.netcracker.profiler.agent.MessageBody;
import com.netcracker.profiler.agent.Profiler;

import com.rabbitmq.client.AMQP;

import java.nio.ByteBuffer;

public class ChannelN {
    /**
     * Records the publish, and the first 100 bytes of the body where the caller supplied one, as
     * {@link MessageBody#describe(byte[], int, String, String)} renders them.
     *
     * <p>The properties are null where the caller passed none: the client substitutes its own
     * defaults inside the method, and the hook receives the argument as the caller passed it.</p>
     */
    public void basicPublish$profiler(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body,
            Throwable throwable) {
        Profiler.event(exchange, "rabbitmq.exchange");
        Profiler.event(routingKey, "rabbitmq.routingKey");
        if (body != null) {
            Profiler.event(
                    MessageBody.describe(body, 100,
                            props == null ? null : props.getContentType(),
                            props == null ? null : props.getContentEncoding()),
                    "rabbitmq.message");
        }
    }

    /**
     * Records the publish for the overload amqp-client 5.31.0 added, reading the body without moving
     * the caller's position.
     *
     * <p>The body is null where the caller published through the {@code byte[]} overload with a null
     * array: from 5.31.0 that overload wraps a non-null array and passes null on unchanged, so an
     * unguarded read here would throw {@code NullPointerException} inside a publish the client
     * accepts. The properties are null where the caller passed none.</p>
     */
    public void basicPublish$profiler(String exchange, String routingKey, AMQP.BasicProperties props, ByteBuffer body,
            Throwable throwable) {
        Profiler.event(exchange, "rabbitmq.exchange");
        Profiler.event(routingKey, "rabbitmq.routingKey");
        if (body != null) {
            Profiler.event(
                    MessageBody.describe(body, 100,
                            props == null ? null : props.getContentType(),
                            props == null ? null : props.getContentEncoding()),
                    "rabbitmq.message");
        }
    }
}
