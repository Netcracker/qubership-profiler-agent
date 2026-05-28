package com.netcracker.profiler.agent.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class HttpServletRequestAdapter {
    private Object httpServletRequest;
    private Method getSession;
    private Method getRequestURL;
    private Method getQueryString;
    private Method getRequestedSessionId;
    private Method getMethod;
    private Method getHeader;
    private Method getCookies;
    private Method setAttribute;

    public HttpServletRequestAdapter(Object httpServletRequest) throws NoSuchMethodException {
        this.httpServletRequest = httpServletRequest;
        // Spring Session wraps the request in a non-public inner class (SessionRepositoryRequestWrapper).
        // Class.getMethod returns a Method whose declaringClass is that wrapper, and Method.invoke
        // performs an access check against the declaring class modifiers — so we must setAccessible(true)
        // even for methods declared public on a public ancestor interface.
        getSession = this.httpServletRequest.getClass().getMethod("getSession", boolean.class);
        getSession.setAccessible(true);
        getRequestURL = this.httpServletRequest.getClass().getMethod("getRequestURL");
        getRequestURL.setAccessible(true);
        getQueryString = this.httpServletRequest.getClass().getMethod("getQueryString");
        getQueryString.setAccessible(true);
        getRequestedSessionId = this.httpServletRequest.getClass().getMethod("getRequestedSessionId");
        getRequestedSessionId.setAccessible(true);
        getMethod = this.httpServletRequest.getClass().getMethod("getMethod");
        getMethod.setAccessible(true);
        getHeader = this.httpServletRequest.getClass().getMethod("getHeader", String.class);
        getHeader.setAccessible(true);
        getCookies = this.httpServletRequest.getClass().getMethod("getCookies");
        getCookies.setAccessible(true);
        setAttribute = this.httpServletRequest.getClass().getMethod("setAttribute", String.class, Object.class);
        setAttribute.setAccessible(true);
    }

    public HttpSessionAdapter getSession(boolean createSession) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Object session = getSession.invoke(httpServletRequest, createSession);
        if(session == null) {
            return null;
        }
        return new HttpSessionAdapter(session);
    }

    public StringBuffer getRequestURL() throws InvocationTargetException, IllegalAccessException {
        return (StringBuffer) getRequestURL.invoke(httpServletRequest);
    }

    public String getQueryString() throws InvocationTargetException, IllegalAccessException {
        return (String) getQueryString.invoke(httpServletRequest);
    }

    public String getRequestedSessionId() throws InvocationTargetException, IllegalAccessException {
        return (String) getRequestedSessionId.invoke(httpServletRequest);
    }

    public String getMethod() throws InvocationTargetException, IllegalAccessException {
        return (String) getMethod.invoke(httpServletRequest);
    }

    public String getHeader(String name) throws InvocationTargetException, IllegalAccessException {
        return (String) getHeader.invoke(httpServletRequest, name);
    }

    public CookieAdapter[] getCookies() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Object[] cookies = (Object[]) getCookies.invoke(httpServletRequest);
        if(cookies == null) {
            return null;
        }
        CookieAdapter[] result = new CookieAdapter[cookies.length];
        for(int i=0; i < cookies.length; i++){
            result[i] = new CookieAdapter(cookies[i]);
        }
        return result;
    }

    public void setAttribute(String name, Object value) throws InvocationTargetException, IllegalAccessException {
        setAttribute.invoke(httpServletRequest, name, value);
    }

}
