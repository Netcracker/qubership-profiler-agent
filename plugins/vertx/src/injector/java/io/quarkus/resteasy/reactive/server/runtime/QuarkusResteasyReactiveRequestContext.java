package io.quarkus.resteasy.reactive.server.runtime;

import io.vertx.ext.web.RoutingContext;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;

public class QuarkusResteasyReactiveRequestContext extends ResteasyReactiveRequestContext {
    public native RoutingContext getContext();
}
