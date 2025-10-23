package com.netcracker.cdt.ui.rest;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.vertx.web.Route;
import io.vertx.ext.web.RoutingContext;

import io.vertx.ext.web.handler.StaticHandler;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@LookupIfProperty(name = "service.type", stringValue = "ui")
@ApplicationScoped
public class StaticRoute {
    @ConfigProperty(name = "service.type", defaultValue="unknown")
    String serviceType;

    // neither path nor regex is set - match a path derived from the method name
    @Route(path = "*", methods = Route.HttpMethod.GET)
    void serviceFilter(RoutingContext rc) {
        // disable GET methods and serving statics for collector
        var serve = "ui".equals(serviceType) ||  "all".equals(serviceType);
        var path = rc.normalizedPath();
        if (path.contains("/q/") || path.contains("/api/") || path.contains("/esc/")) { // but should work with API and /q/ (actuator)
            serve = true;
        }
        if (!serve) {
            rc.response().end("OK"); // just mock actual responses
        } else {
            StaticHandler.create().handle(rc);
        }
    }

}
