package io.undertow.servlet.handlers;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.http.HttpServletLogUtils;
import com.netcracker.profiler.agent.http.ServletRequestAdapter;

import javax.servlet.ServletRequest;

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
