package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_rabbitmq extends EnhancerPlugin {

    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("queue").index(true);
        configuration.getParameterInfo("rabbitmq.url").index(true);
    }
}
