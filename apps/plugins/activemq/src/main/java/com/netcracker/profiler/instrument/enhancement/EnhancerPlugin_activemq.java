package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

public class EnhancerPlugin_activemq extends EnhancerPlugin {
    @Override
    public void init(Element node, Configuration_01 conf) {
        super.init(node, conf);

        conf.getParameterInfo("jms.consumer").index(true);
        conf.getParameterInfo("jms.messageid").index(true);
        conf.getParameterInfo("jms.correlationid").index(true);
        conf.getParameterInfo("jms.destination").index(true);
        conf.getParameterInfo("jms.replyto").index(true);
        conf.getParameterInfo("jms.text.fragment").index(true);
        conf.getParameterInfo("jms.text").big(true);
        conf.getParameterInfo("jms.timestamp");
    }
}
