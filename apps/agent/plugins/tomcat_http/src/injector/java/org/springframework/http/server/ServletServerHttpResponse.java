package org.springframework.http.server;

public class ServletServerHttpResponse implements ServerHttpResponse {
    public native javax.servlet.http.HttpServletResponse getServletResponse();
}
