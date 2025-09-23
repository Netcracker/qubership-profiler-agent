package com.netcracker.profiler.agent.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CookieAdapter {
    private Object cookie;
    private Method getName;
    private Method getValue;

    public CookieAdapter(Object cookie) throws NoSuchMethodException {
        this.cookie = cookie;
        this.getName = cookie.getClass().getDeclaredMethod("getName");
        this.getValue = cookie.getClass().getDeclaredMethod("getValue");
    }

    public String getName() throws InvocationTargetException, IllegalAccessException {
        return (String)getName.invoke(cookie);
    }

    public String getValue() throws InvocationTargetException, IllegalAccessException {
        return (String)getValue.invoke(cookie);
    }
}
