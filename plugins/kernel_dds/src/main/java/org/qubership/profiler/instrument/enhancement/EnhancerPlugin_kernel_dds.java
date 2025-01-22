package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Configuration_01;
import org.w3c.dom.Element;

public class EnhancerPlugin_kernel_dds extends EnhancerPlugin {
    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("current.domain").index(true);
    }
}
