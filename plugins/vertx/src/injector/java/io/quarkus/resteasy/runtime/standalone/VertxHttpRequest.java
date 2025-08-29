package io.quarkus.resteasy.runtime.standalone;

import org.jboss.resteasy.specimpl.ResteasyUriInfo;

public class VertxHttpRequest {
    public native String getHttpMethod();
    public native String getRemoteAddress();
    public native ResteasyUriInfo getUri();
}
