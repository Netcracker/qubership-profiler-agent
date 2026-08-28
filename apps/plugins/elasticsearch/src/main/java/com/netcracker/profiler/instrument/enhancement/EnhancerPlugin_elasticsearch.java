package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_elasticsearch extends EnhancerPlugin {
    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("async.emitted").index(true).list(false);
        configuration.getParameterInfo("async.absorbed").index(true).list(false);
        configuration.getParameterInfo("es.query.path").index(true).list(true);
    }
}
