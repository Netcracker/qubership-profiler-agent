package com.netcracker.profiler.filter;

import java.io.IOException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CachingFilter implements Filter {
    public void destroy() {
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws ServletException, IOException {
        if (resp instanceof HttpServletResponse) {
            HttpServletResponse res = (HttpServletResponse) resp;
            HttpServletRequest request = (HttpServletRequest) req;
            if (request.getRequestURI().contains(".cache.")) {
                res.setHeader("Cache-Control", "public, max-age=36000");
            } else {
                res.setHeader("Cache-Control", "no-cache,no-store,must-revalidate");
                res.setHeader("Vary", "*");
            }
        }
        chain.doFilter(req, resp);
    }

    public void init(FilterConfig config) throws ServletException {
    }
}
