package io.undertow.servlet.handlers;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.http.HttpServletLogUtils;
import org.qubership.profiler.agent.http.ServletRequestAdapter;

import jakarta.servlet.ServletRequest;

public class FilterHandler {
    @SuppressWarnings("UnusedNestedClass")
    private static class FilterChainImpl {
        void fillNcUser$profiler(ServletRequest request) {
            try {
                HttpServletLogUtils.fillNcUser(new ServletRequestAdapter(request));
            } catch (Throwable e) {
                Profiler.pluginException(e);
            }
        }
    }
}
