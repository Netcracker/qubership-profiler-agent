package org.springframework.amqp.rabbit.listener.adapter;


import com.netcracker.profiler.agent.Profiler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.Message;

public class MessagingMessageListenerAdapter {

    private void invokeHandler$profiler(org.springframework.amqp.core.Message amqpMessage,
                                                      Channel channel,
                                                      Message<?> message,
                                                      Throwable throwable) {
        MessageProperties messageProperties = amqpMessage.getMessageProperties();
        Profiler.event(messageProperties.getConsumerQueue(), "queue");
        Connection connection = channel.getConnection();
        Profiler.event(connection.getAddress().getHostAddress() + ":" + connection.getPort(), "rabbitmq.url");
    }
}
