package org.springframework.amqp.core;

public class MessageProperties {
    public native String getConsumerQueue();
    public native String getReceivedExchange();
}
