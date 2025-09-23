package com.rabbitmq.client.impl;

import com.netcracker.profiler.agent.Profiler;

public class ChannelN {
    public void basicPublish$profiler(String exchange, String routingKey, byte[] body, Throwable throwable) {
        Profiler.event(exchange, "rabbitmq.exchange");
        Profiler.event(routingKey, "rabbitmq.routingKey");
        int length = Math.min(body.length, 100);
        Profiler.event(new String(body, 0, length), "rabbitmq.message");
    }
}
