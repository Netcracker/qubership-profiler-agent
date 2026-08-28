package io.netty.channel;

import java.net.SocketAddress;

public interface Channel {
    SocketAddress localAddress();

    SocketAddress remoteAddress();
}
