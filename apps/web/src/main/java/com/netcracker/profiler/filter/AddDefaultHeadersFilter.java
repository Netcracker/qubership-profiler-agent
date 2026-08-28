package com.netcracker.profiler.filter;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class AddDefaultHeadersFilter implements Filter {
    public void destroy() {
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws ServletException, IOException {
        if (resp instanceof HttpServletResponse) {
            HttpServletResponse res = (HttpServletResponse) resp;
            if (!res.containsHeader("Content-Security-Policy")) {
                res.setHeader("Content-Security-Policy", "frame-ancestors 'none'");
            }
            res.setHeader("X-Frame-Options", "SAMEORIGIN");
            res.setHeader("X-XSS-Protection", "0");
            res.setHeader("X-content-Type-options", "nosniff");
        }
        chain.doFilter(req, resp);
    }

    public void init(FilterConfig config) throws ServletException {
    }
}
