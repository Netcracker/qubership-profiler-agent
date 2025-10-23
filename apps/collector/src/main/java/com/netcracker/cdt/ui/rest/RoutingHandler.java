package com.netcracker.cdt.ui.rest;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.vertx.web.RouteFilter;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Set;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class RoutingHandler {
    Set<String> UI_ROUTES = Set.of("/calls", "/pods-info", "/heap-dumps");

    @RouteFilter(100)
    void myFilter(RoutingContext rc) {
        if (HttpMethod.GET.equals(rc.request().method())) {
            String path = resolvePath(rc);
            if (UI_ROUTES.contains(path)) {
                rc.reroute(rc.mountPoint() != null ? rc.mountPoint() : "/");
                return;
            }
        }
        rc.next();
    }

    static String resolvePath(RoutingContext ctx) {
        var mp = ctx.mountPoint();
        if (mp == null) {
            return ctx.normalizedPath();
        }
        return ctx.normalizedPath().substring(mp.endsWith("/") ? mp.length() - 1 : mp.length());
    }
}
