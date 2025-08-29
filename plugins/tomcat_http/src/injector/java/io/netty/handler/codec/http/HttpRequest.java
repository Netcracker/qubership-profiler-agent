package io.netty.handler.codec.http;

public interface HttpRequest {
    String uri();

    HttpMethod method();
}
