package org.springframework.http.server;

public class ServletServerHttpResponse implements ServerHttpResponse {
    public native jakarta.servlet.http.HttpServletResponse getServletResponse();
}
