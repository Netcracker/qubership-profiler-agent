package org.jboss.resteasy.core;

import org.qubership.profiler.agent.Profiler;

import io.quarkus.resteasy.runtime.standalone.VertxHttpRequest;
import jakarta.ws.rs.core.HttpHeaders;
import org.jboss.resteasy.spi.HttpRequest;

public class MethodInjectorImpl {

    public void invoke$profiler(HttpRequest request) {

        if (!(request instanceof VertxHttpRequest)) {
            return;
        }

        VertxHttpRequest httpRequest = (VertxHttpRequest) request;

        String webMethod = httpRequest.getHttpMethod();
        Profiler.event(webMethod, "web.method");

        String uri = httpRequest.getUri().getRequestUri().toString();
        String[] split = uri.split("[?]");
        Profiler.event(split[0], "web.url");
        if (split.length == 2) {
            Profiler.event(split[1], "web.query");
        }

        String remoteAddress = httpRequest.getRemoteAddress();
        Profiler.event(remoteAddress, "web.remote.addr");
        Profiler.event(remoteAddress, "web.remote.host");

        HttpHeaders headers = request.getHttpHeaders();
        Profiler.event(headers.getHeaderString("Referer"), "_web.referer");
        Profiler.event(headers.getHeaderString("dynatrace"), "dynatrace");
        Profiler.event(headers.getHeaderString("x-client-transaction-id"), "x-client-transaction-id");
        Profiler.event(headers.getHeaderString("X-B3-TraceId"), "X-B3-TraceId");
        Profiler.event(headers.getHeaderString("X-B3-ParentSpanId"), "X-B3-ParentSpanId");
        Profiler.event(headers.getHeaderString("X-B3-SpanId"), "X-B3-SpanId");
        Profiler.event(headers.getHeaderString("x-request-id"), "x-request-id");
    }

}
