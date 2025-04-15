package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.http.HttpServletLogUtils;
import org.qubership.profiler.agent.http.ServletRequestAdapter;

import jakarta.servlet.ServletRequest;

public class Tomcat10HTTPEnhancer {

    public static void dumpRequest$profiler(ServletRequest request) {
        try {
            HttpServletLogUtils.dumpRequest(new ServletRequestAdapter(request));
        } catch (Throwable e) {
            Profiler.pluginException(e);
        }
    }

    public static void afterRequest$profiler(ServletRequest request) {
        try {
            HttpServletLogUtils.afterRequest(new ServletRequestAdapter(request));
        } catch (Throwable e) {
            Profiler.pluginException(e);
        }
    }
}
