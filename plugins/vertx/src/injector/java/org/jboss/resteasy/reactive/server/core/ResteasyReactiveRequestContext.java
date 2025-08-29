package org.jboss.resteasy.reactive.server.core;

import org.jboss.resteasy.reactive.server.jaxrs.HttpHeadersImpl;

public class ResteasyReactiveRequestContext {

    public native String getMethod();

    public native String getAbsoluteURI();

    public native HttpHeadersImpl getHttpHeaders();
}
