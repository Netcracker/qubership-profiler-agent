package com.netcracker.profiler.agent.http;

import com.netcracker.profiler.agent.ESCLogger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ServletRequestAdapter {
    private static final ESCLogger logger = ESCLogger.getLogger(ServletRequestAdapter.class);
    private Object servletRequest;
    private static final ClassValue<Boolean> IS_HTTP_REQUEST = new IsHttpRequestClassValue();
    private Method getRemoteAddr;
    private Method getRemoteHost;
    private Method setAttribute;

    public ServletRequestAdapter(Object servletRequest) throws ClassNotFoundException, NoSuchMethodException {
        this.servletRequest = servletRequest;

        try {
            getRemoteAddr = servletRequest.getClass().getMethod("getRemoteAddr");
        } catch (NoSuchMethodException e) {
            logger.severe("ServletRequest should have method getRemoteAddr", e);
        }

        try {
            getRemoteHost = servletRequest.getClass().getMethod("getRemoteHost");
        } catch (NoSuchMethodException e) {
            logger.severe("ServletRequest should have method getRemoteHost", e);
        }

        try {
            setAttribute = servletRequest.getClass().getMethod("setAttribute", String.class, Object.class);
        } catch (NoSuchMethodException e) {
            logger.severe("ServletRequest should have method setAttribute", e);
        }
    }

    public HttpServletRequestAdapter toHttpServletRequestAdapter() throws NoSuchMethodException {
        return new HttpServletRequestAdapter(servletRequest);
    }

    public boolean isHttpServetRequest() {
        return IS_HTTP_REQUEST.get(servletRequest.getClass());
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

    private static class IsHttpRequestClassValue extends ClassValue<Boolean> {
        @Override
        protected Boolean computeValue(Class<?> type) {
            try {
                Class<?> httpServletRequestClass = Class.forName("javax.servlet.http.HttpServletRequest", false, type.getClassLoader());
                if (httpServletRequestClass.isAssignableFrom(type)) {
                    return true;
                }
            } catch (ClassNotFoundException e) {
                logger.fine("Package javax.servlet doesn't available. It seems that will use Jakarta EE.");
            }

            try {
                Class<?> httpServletRequestClass = Class.forName("jakarta.servlet.http.HttpServletRequest", false, type.getClassLoader());
                if (httpServletRequestClass.isAssignableFrom(type)) {
                    return true;
                }
            } catch (ClassNotFoundException e) {
                logger.fine("Package jakarta.servlet doesn't available. It seems that will use Java EE.");
            }
            return false;
        }
    }
}
