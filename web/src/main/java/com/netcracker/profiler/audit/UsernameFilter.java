package com.netcracker.profiler.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class UsernameFilter implements Filter {
    public static final String PROFILER_REMOTE_USERNAME = "remote.username";

    protected final Logger log = LoggerFactory.getLogger(UsernameFilter.class);

    public void init(FilterConfig filterConfig) throws ServletException {
    }

    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        try {
            MDC.put("req.remoteAddr", servletRequest.getRemoteAddr());
            if (servletRequest instanceof HttpServletRequest) {
                HttpServletRequest req = (HttpServletRequest) servletRequest;
                HttpSession session = req.getSession();
                String remoteUser = req.getRemoteUser();
                if (remoteUser == null && session != null)
                    remoteUser = (String) session.getAttribute(PROFILER_REMOTE_USERNAME);
                MDC.put("req.remoteUser", remoteUser);
                if (session != null && remoteUser != null && session.getAttribute(PROFILER_REMOTE_USERNAME) == null) {
                    session.setAttribute(PROFILER_REMOTE_USERNAME, remoteUser);
                    log.info("login");
                }
                log.trace("page accessed {}?{}", req.getRequestURI(), req.getQueryString());
            }

            filterChain.doFilter(servletRequest, servletResponse);
        } finally {
            MDC.remove("req.remoteUser");
            MDC.remove("req.remoteAddr");
        }
    }

    public void destroy() {
    }
}
