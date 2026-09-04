package com.rabbitmq.client.impl;

import com.netcracker.profiler.agent.Profiler;

import java.nio.ByteBuffer;

public class ChannelN {
    public void basicPublish$profiler(String exchange, String routingKey, byte[] body, Throwable throwable) {
        Profiler.event(exchange, "rabbitmq.exchange");
        Profiler.event(routingKey, "rabbitmq.routingKey");
        if (body != null) {
            int length = Math.min(body.length, 100);
            Profiler.event(new String(body, 0, length), "rabbitmq.message");
        }
    }

    public void basicPublish$profiler(String exchange, String routingKey, ByteBuffer body, Throwable throwable) {
        Profiler.event(exchange, "rabbitmq.exchange");
        Profiler.event(routingKey, "rabbitmq.routingKey");
        if (body != null) {
            ByteBuffer duplicate = body.duplicate();
            byte[] bytes = new byte[Math.min(duplicate.remaining(), 100)];
            duplicate.get(bytes);
            Profiler.event(new String(bytes), "rabbitmq.message");
        }
    }
}
