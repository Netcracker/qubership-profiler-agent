package com.netcracker.profiler.agent.http;

import com.netcracker.profiler.agent.ESCLogger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ServletRequestAdapter {
    private static final ESCLogger logger = ESCLogger.getLogger(ServletRequestAdapter.class);
    private Object servletRequest;
    private Class javaxHttpServletRequestClass;
    private Class jakartaHttpServletRequestClass;
    private Method getRemoteAddr;
    private Method getRemoteHost;
    private Method setAttribute;

    public ServletRequestAdapter(Object servletRequest) throws ClassNotFoundException, NoSuchMethodException {
        this.servletRequest = servletRequest;

        try {
            javaxHttpServletRequestClass = Class.forName("javax.servlet.http.HttpServletRequest", false, servletRequest.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            logger.fine("Package javax.servlet doesn't available. It seems that will use Jakarta EE.");
        }

        try {
            jakartaHttpServletRequestClass = Class.forName("jakarta.servlet.http.HttpServletRequest", false, servletRequest.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            logger.fine("Package jakarta.servlet doesn't available. It seems that will use Java EE.");
        }

        try {
            getRemoteAddr = servletRequest.getClass().getMethod("getRemoteAddr");
        } catch (NoSuchMethodException e) {
            logger.severe("", e);
        }

        try {
            getRemoteHost = servletRequest.getClass().getMethod("getRemoteHost");
        } catch (NoSuchMethodException e) {
            logger.severe("", e);
        }

        try {
            setAttribute = servletRequest.getClass().getMethod("setAttribute", String.class, Object.class);
        } catch (NoSuchMethodException e) {
            logger.severe("", e);
        }
    }

    public HttpServletRequestAdapter toHttpServletRequestAdapter() throws NoSuchMethodException {
        return new HttpServletRequestAdapter(servletRequest);
    }

    public boolean isHttpServetRequest() {
        if (javaxHttpServletRequestClass != null) {
            return javaxHttpServletRequestClass.isAssignableFrom(servletRequest.getClass());
        }

        if (jakartaHttpServletRequestClass != null) {
            return jakartaHttpServletRequestClass.isAssignableFrom(servletRequest.getClass());
        }

        return false;
    }

    public String getRemoteAddr() throws InvocationTargetException, IllegalAccessException {
        if (getRemoteAddr == null) {
            return "unknown";
        }
        return (String) getRemoteAddr.invoke(servletRequest);
    }

    public String getRemoteHost() throws InvocationTargetException, IllegalAccessException {
        if (getRemoteHost == null) {
            return "unknown";
        }
        return (String) getRemoteHost.invoke(servletRequest);
    }

    public void setAttribute(String name, Object value) throws InvocationTargetException, IllegalAccessException {
        if (setAttribute == null) {
            return;
        }
        setAttribute.invoke(servletRequest, name, value);
    }
}
