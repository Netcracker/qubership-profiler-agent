package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.util.NaturalComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class EnhancerPlugin_jetty_http extends EnhancerPlugin {

    private final static Logger log = LoggerFactory.getLogger(EnhancerPlugin_jetty_http.class);

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
        configuration.getParameterInfo("x-version").index(true);
        configuration.getParameterInfo("x-version-name").index(true);
    }

    @Override
    public boolean accept(ClassInfo info) {
        String version = getVersion(info);
        String jarName = info.getJarName();
        if (version == null) {
            log.info("Cannot resolve version for {}", info.getJarName());
            return false;
        }

        log.debug("Class name: {}, jar name: {}", info.getClassName(), jarName);

        boolean versionCheck = NaturalComparator.INSTANCE.compare(version, "10") < 0;
        log.debug("Jetty jar version is {}, run jetty_http plugins = {}", version, versionCheck);
        return versionCheck;
    }

    private String getVersion(ClassInfo info) {
        String version = info.getJarAttribute("Implementation-Version");
        if(version == null) {
            version = info.getJarAttribute("Bundle-Version");
        }
        return version;
    }
}
