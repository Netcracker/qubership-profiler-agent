package com.netcracker.profiler.agent.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CookieAdapter {
    private Object cookie;
    private Method getName;
    private Method getValue;

    public CookieAdapter(Object cookie) throws NoSuchMethodException {
        this.cookie = cookie;
        // getMethod (not getDeclaredMethod) walks the inheritance chain — works even when the
        // concrete cookie class doesn't override the accessors. setAccessible bypasses the language
        // access check when the declaring class is non-public (see HttpServletRequestAdapter).
        this.getName = cookie.getClass().getMethod("getName");
        this.getName.setAccessible(true);
        this.getValue = cookie.getClass().getMethod("getValue");
        this.getValue.setAccessible(true);
    }

    public String getName() throws InvocationTargetException, IllegalAccessException {
        return (String)getName.invoke(cookie);
    }

    public String getValue() throws InvocationTargetException, IllegalAccessException {
        return (String)getValue.invoke(cookie);
    }
}
