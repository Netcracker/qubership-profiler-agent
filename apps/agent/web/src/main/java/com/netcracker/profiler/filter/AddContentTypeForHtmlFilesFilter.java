package com.netcracker.profiler.filter;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class AddContentTypeForHtmlFilesFilter implements Filter {
    public void destroy() {
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws ServletException, IOException {
        if (resp instanceof HttpServletResponse) {
            HttpServletResponse res = (HttpServletResponse) resp;
            res.setContentType("text/html");
        }
        chain.doFilter(req, resp);
    }

    public void init(FilterConfig config) throws ServletException {
    }
}
