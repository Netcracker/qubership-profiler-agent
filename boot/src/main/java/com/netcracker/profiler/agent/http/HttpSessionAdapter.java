package com.netcracker.profiler.agent.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class HttpSessionAdapter {
    private Object httpSession ;
    private Method getAttribute;

    public HttpSessionAdapter(Object httpSession) throws NoSuchMethodException {
        this.httpSession = httpSession;
        getAttribute = this.httpSession.getClass().getMethod("getAttribute", String.class);
    }

    public Object getAttribute(String name) throws InvocationTargetException, IllegalAccessException {
        return getAttribute.invoke(httpSession, name);
    }
}
