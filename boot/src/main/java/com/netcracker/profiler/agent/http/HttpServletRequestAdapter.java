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
        getSession = this.httpServletRequest.getClass().getMethod("getSession", boolean.class);
        getRequestURL = this.httpServletRequest.getClass().getMethod("getRequestURL");
        getQueryString = this.httpServletRequest.getClass().getMethod("getQueryString");
        getRequestedSessionId = this.httpServletRequest.getClass().getMethod("getRequestedSessionId");
        getMethod = this.httpServletRequest.getClass().getMethod("getMethod");
        getHeader = this.httpServletRequest.getClass().getMethod("getHeader", String.class);
        getCookies = this.httpServletRequest.getClass().getMethod("getCookies");
        setAttribute = this.httpServletRequest.getClass().getMethod("setAttribute", String.class, Object.class);
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
