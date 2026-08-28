package io.vertx.ext.web;

import io.vertx.core.http.HttpServerRequest;

public interface RoutingContext {

    HttpServerRequest request();
}
