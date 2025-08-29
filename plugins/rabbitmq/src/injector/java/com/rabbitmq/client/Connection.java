package com.rabbitmq.client;

import java.net.InetAddress;

public interface Connection {
    InetAddress getAddress();
    int getPort();
}
