package com.liferay.portlet.layoutconfiguration.util;

import org.qubership.profiler.agent.Profiler;

import java.security.Principal;

import javax.portlet.RenderRequest;
import javax.servlet.http.HttpServletRequest;

public class RuntimePortletUtil {
    private static void savePortletId$profiler(HttpServletRequest request, RenderRequest renderRequest, String portletId) {
        Profiler.event(portletId, "portlet.id");
        if (request.getAttribute("nc.execution-statistics-collector.user.saved") != null) {
            return;
        }
        Principal principal = renderRequest.getUserPrincipal();
        if (principal != null) {
            Profiler.event("pu:" + principal.getName(), "portlet.user.id");
            request.setAttribute("nc.execution-statistics-collector.user.saved", Boolean.TRUE);
        }
    }
}
