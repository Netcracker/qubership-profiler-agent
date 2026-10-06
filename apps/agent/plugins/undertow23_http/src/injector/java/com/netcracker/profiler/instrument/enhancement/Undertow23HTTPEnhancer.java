package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.http.HttpServletLogUtils;
import com.netcracker.profiler.agent.http.ServletRequestAdapter;

import jakarta.servlet.ServletRequest;

public class Undertow23HTTPEnhancer {

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
