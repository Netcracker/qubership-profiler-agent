package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_test extends EnhancerPlugin {
    @Override
    public void init(Element node, Configuration_01 conf) {
        super.init(node, conf);
    }
}
