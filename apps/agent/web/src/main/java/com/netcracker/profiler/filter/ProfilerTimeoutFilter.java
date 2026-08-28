package com.netcracker.profiler.filter;

import com.netcracker.profiler.timeout.ProfilerTimeoutException;
import com.netcracker.profiler.timeout.ProfilerTimeoutHandler;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;

@Singleton
public class ProfilerTimeoutFilter implements Filter {
    public void destroy() {
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws ServletException, IOException {
        ProfilerTimeoutHandler.scheduleTimeout();
        try {
            chain.doFilter(req, resp);
        } catch (ProfilerTimeoutException e) {
            if(resp instanceof HttpServletResponse) {
                HttpServletResponse res = (HttpServletResponse) resp;
                if(!res.isCommitted()) {
                    res.sendError(408); //408 Request Timeout
                }
            }
        } finally {
            ProfilerTimeoutHandler.cancelTimeout();
        }
    }

    public void init(FilterConfig config) throws ServletException {
    }

}
