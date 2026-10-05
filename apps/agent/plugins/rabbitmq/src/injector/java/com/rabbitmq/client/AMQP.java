package com.rabbitmq.client;

public interface AMQP {
    class BasicProperties {
        public native String getContentType();
        public native String getContentEncoding();
    }
}
