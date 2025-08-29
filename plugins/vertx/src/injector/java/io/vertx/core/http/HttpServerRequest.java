package io.vertx.core.http;

import io.vertx.core.net.SocketAddress;

public interface HttpServerRequest {

    SocketAddress remoteAddress();
}
