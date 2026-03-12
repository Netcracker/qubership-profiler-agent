package com.netcracker.profiler.security;

import java.io.IOException;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class AbstractSecurityFilter implements Filter {

    protected final DummySecurityService securityService;

    protected AbstractSecurityFilter(DummySecurityService securityService) {
        this.securityService = securityService;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public final void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain filterChain) throws IOException, ServletException {
        if (securityService.isSecurityEnabled() && servletRequest instanceof HttpServletRequest) {
            doFilter((HttpServletRequest) servletRequest, (HttpServletResponse) servletResponse, filterChain);
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    protected abstract void doFilter(HttpServletRequest httpRequest, HttpServletResponse httpResponse,
                                     FilterChain filterChain) throws IOException, ServletException;

    @Override
    public void destroy() {

    }
}
