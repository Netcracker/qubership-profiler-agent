package org.qubership.profiler.filter;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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
