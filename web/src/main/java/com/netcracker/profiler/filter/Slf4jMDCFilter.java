package com.netcracker.profiler.filter;

import ch.qos.logback.classic.ClassicConstants;
import org.slf4j.MDC;

import java.io.IOException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;

public class Slf4jMDCFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        setMdc(servletRequest);
        try {
            filterChain.doFilter(servletRequest, servletResponse);
        } finally {
            clearMdc();
        }
    }

    void setMdc(ServletRequest request) {
        MDC.put(ClassicConstants.REQUEST_REMOTE_HOST_MDC_KEY, request.getRemoteHost());

        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpServletRequest = (HttpServletRequest) request;
            MDC.put(ClassicConstants.REQUEST_REQUEST_URI, httpServletRequest.getRequestURI());
            StringBuffer requestURL = httpServletRequest.getRequestURL();
            if (requestURL != null) {
                MDC.put(ClassicConstants.REQUEST_REQUEST_URL, requestURL.toString());
            }
            MDC.put(ClassicConstants.REQUEST_METHOD, httpServletRequest.getMethod());
            MDC.put(ClassicConstants.REQUEST_QUERY_STRING, httpServletRequest.getQueryString());
            MDC.put(ClassicConstants.REQUEST_USER_AGENT_MDC_KEY, httpServletRequest.getHeader("User-Agent"));
            MDC.put(ClassicConstants.REQUEST_X_FORWARDED_FOR, httpServletRequest.getHeader("X-Forwarded-For"));
        }
    }

    private void clearMdc() {
        MDC.remove(ClassicConstants.REQUEST_REMOTE_HOST_MDC_KEY);
        MDC.remove(ClassicConstants.REQUEST_REQUEST_URI);
        MDC.remove(ClassicConstants.REQUEST_QUERY_STRING);
        MDC.remove(ClassicConstants.REQUEST_REQUEST_URL);
        MDC.remove(ClassicConstants.REQUEST_METHOD);
        MDC.remove(ClassicConstants.REQUEST_USER_AGENT_MDC_KEY);
        MDC.remove(ClassicConstants.REQUEST_X_FORWARDED_FOR);
    }

    @Override
    public void destroy() {
    }
}
