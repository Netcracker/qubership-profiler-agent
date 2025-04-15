package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_http extends EnhancerPlugin {
    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("web.url").index(true);
        configuration.getParameterInfo("_web.referer").index(true);
        configuration.getParameterInfo("web.method").index(true);
        configuration.getParameterInfo("web.query").index(true);
        configuration.getParameterInfo("web.session.id").index(true);
        configuration.getParameterInfo("web.remote.addr").index(true);
        configuration.getParameterInfo("dynatrace").index(true);
        configuration.getParameterInfo("x-client-transaction-id").index(true);
        configuration.getParameterInfo("X-B3-TraceId").index(true);
        configuration.getParameterInfo("X-B3-ParentSpanId").index(true);
        configuration.getParameterInfo("X-B3-SpanId").index(true);
        configuration.getParameterInfo("x-request-id").index(true);
    }
}
