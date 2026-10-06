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

    /**
     * Records the consumer queue and the connection address for spring-amqp 4, where the handler
     * takes the AMQP messages last and as a varargs array.
     *
     * <p>A batch listener passes several messages, which arrive on one queue, so the first one names
     * it.</p>
     */
    private void invokeHandler$profiler(Channel channel,
                                                      org.springframework.amqp.core.Message[] amqpMessages,
                                                      Throwable throwable) {
        if (amqpMessages != null && amqpMessages.length > 0) {
            MessageProperties messageProperties = amqpMessages[0].getMessageProperties();
            Profiler.event(messageProperties.getConsumerQueue(), "queue");
        }
        Connection connection = channel.getConnection();
        Profiler.event(connection.getAddress().getHostAddress() + ":" + connection.getPort(), "rabbitmq.url");
    }
}
