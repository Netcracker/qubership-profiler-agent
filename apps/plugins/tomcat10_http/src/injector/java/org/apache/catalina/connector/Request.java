package org.apache.catalina.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import jakarta.servlet.*;
import jakarta.servlet.http.*;

public class Request implements jakarta.servlet.http.HttpServletRequest {
    public native String getAuthType();

    //@Override
    public native Cookie[] getCookies();

    //@Override
    public native long getDateHeader(String s);

    //@Override
    public native String getHeader(String s);

    //@Override
    public native Enumeration<String> getHeaders(String s);

    //@Override
    public native Enumeration<String> getHeaderNames();

    //@Override
    public native int getIntHeader(String s);

    //@Override
    public native String getMethod();

    //@Override
    public native String getPathInfo();

    //@Override
    public native String getPathTranslated();

    //@Override
    public native String getContextPath();

    //@Override
    public native String getQueryString();

    //@Override
    public native String getRemoteUser();

    //@Override
    public native boolean isUserInRole(String s);

    //@Override
    public native Principal getUserPrincipal();

    //@Override
    public native String getRequestedSessionId();

    //@Override
    public native String getRequestURI();

    //@Override
    public native StringBuffer getRequestURL();

    //@Override
    public native String getServletPath();

    //@Override
    public native HttpSession getSession(boolean b);

    //@Override
    public native HttpSession getSession();

    //@Override
    public native String changeSessionId();

    //@Override
    public native boolean isRequestedSessionIdValid();

    //@Override
    public native boolean isRequestedSessionIdFromCookie();

    //@Override
    public native boolean isRequestedSessionIdFromURL();

    //@Override
    public native boolean isRequestedSessionIdFromUrl();

    //@Override
    public native boolean authenticate(HttpServletResponse httpServletResponse) throws IOException, ServletException;

    //@Override
    public native void login(String s, String s1) throws ServletException;

    //@Override
    public native void logout() throws ServletException;

    //@Override
    public native Collection<Part> getParts() throws IOException, ServletException;

    //@Override
    public native Part getPart(String s) throws IOException, ServletException;

    //@Override
    public native <T extends HttpUpgradeHandler> T upgrade(Class<T> aClass) throws IOException, ServletException;

    //@Override
    public native Object getAttribute(String s);

    //@Override
    public native Enumeration<String> getAttributeNames();

    //@Override
    public native String getCharacterEncoding();

    //@Override
    public native void setCharacterEncoding(String s) throws UnsupportedEncodingException;

    //@Override
    public native int getContentLength();

    //@Override
    public native long getContentLengthLong();

    //@Override
    public native String getContentType();

    //@Override
    public native ServletInputStream getInputStream() throws IOException;

    //@Override
    public native String getParameter(String s);

    //@Override
    public native Enumeration<String> getParameterNames();

    //@Override
    public native String[] getParameterValues(String s);

    //@Override
    public native Map<String, String[]> getParameterMap();

    //@Override
    public native String getProtocol();

    //@Override
    public native String getScheme();

    //@Override
    public native String getServerName();

    //@Override
    public native int getServerPort();

    //@Override
    public native BufferedReader getReader() throws IOException;

    //@Override
    public native String getRemoteAddr();

    //@Override
    public native String getRemoteHost();

    //@Override
    public native void setAttribute(String s, Object o);

    //@Override
    public native void removeAttribute(String s);

    //@Override
    public native Locale getLocale();

    //@Override
    public native Enumeration<Locale> getLocales();

    //@Override
    public native boolean isSecure();

    //@Override
    public native RequestDispatcher getRequestDispatcher(String s);

    //@Override
    public native String getRealPath(String s);

    //@Override
    public native int getRemotePort();

    //@Override
    public native String getLocalName();

    //@Override
    public native String getLocalAddr();

    //@Override
    public native int getLocalPort();

    //@Override
    public native ServletContext getServletContext();

    //@Override
    public native AsyncContext startAsync() throws IllegalStateException;

    //@Override
    public native AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) throws IllegalStateException;

    //@Override
    public native boolean isAsyncStarted();

    //@Override
    public native boolean isAsyncSupported();

    //@Override
    public native AsyncContext getAsyncContext();

    //@Override
    public native DispatcherType getDispatcherType();

    //@Override
    public native String getRequestId();

    //@Override
    public native String getProtocolRequestId();

    //@Override
    public native ServletConnection getServletConnection();
}
