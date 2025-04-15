package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_quartz extends EnhancerPlugin {
    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("job.id").index(true);
        configuration.getParameterInfo("job.name").index(true);
        configuration.getParameterInfo("job.action.type").index(true);
        configuration.getParameterInfo("job.class").index(true);
        configuration.getParameterInfo("job.method").index(true);
        configuration.getParameterInfo("job.jms.connection.factory").index(true);
        configuration.getParameterInfo("job.jms.topic").index(true);
        configuration.getParameterInfo("job.url").index(true);
    }
}
