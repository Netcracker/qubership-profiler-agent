package org.jboss.resteasy.reactive.server.handlers;

import org.qubership.profiler.agent.Profiler;

import io.quarkus.resteasy.reactive.server.runtime.QuarkusResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.jaxrs.HttpHeadersImpl;

public class InvocationHandler {

    public void handle$profiler(ResteasyReactiveRequestContext requestContext) {

        if (!(requestContext instanceof QuarkusResteasyReactiveRequestContext)) {
            return;
        }

        String webMethod = requestContext.getMethod();
        Profiler.event(webMethod, "web.method");

        String uri = requestContext.getAbsoluteURI();
        String[] split = uri.split("[?]");
        Profiler.event(split[0], "web.url");
        if (split.length == 2) {
            Profiler.event(split[1], "web.query");
        }

        String remoteAddress = ((QuarkusResteasyReactiveRequestContext) requestContext).getContext().request().remoteAddress().hostAddress();
        Profiler.event(remoteAddress, "web.remote.addr");
        Profiler.event(remoteAddress, "web.remote.host");

        HttpHeadersImpl headers = requestContext.getHttpHeaders();
        Profiler.event(headers.getHeaderString("Referer"), "_web.referer");
        Profiler.event(headers.getHeaderString("dynatrace"), "dynatrace");
        Profiler.event(headers.getHeaderString("x-client-transaction-id"), "x-client-transaction-id");
        Profiler.event(headers.getHeaderString("X-B3-TraceId"), "X-B3-TraceId");
        Profiler.event(headers.getHeaderString("X-B3-ParentSpanId"), "X-B3-ParentSpanId");
        Profiler.event(headers.getHeaderString("X-B3-SpanId"), "X-B3-SpanId");
        Profiler.event(headers.getHeaderString("x-request-id"), "x-request-id");
    }
}
