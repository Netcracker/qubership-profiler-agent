package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancerPlugin_vertx extends EnhancerPlugin {

    private final static Logger log = LoggerFactory.getLogger(EnhancerPlugin_vertx.class);

    private static final Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

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

        String jarName = info.getJarName();
        String version = getVersion(info);
        log.debug("Class name: {}, jar name: {}", info.getClassName(), jarName);
        if (jarName == null) {
            return false;
        }

        if (jarName.contains("resteasy-core")) {

            Matcher m = p.matcher(version);
            if (!m.find()) {
                return false;
            }

            int majorVersion = Integer.parseInt(m.group(1));
            return majorVersion == 6;
        }

        if (jarName.contains("resteasy-reactive")) {

            Matcher m = p.matcher(version);
            if (!m.find()) {
                return false;
            }

            int majorVersion = Integer.parseInt(m.group(1));
            return majorVersion == 3;
        }

        log.debug("Jar has no 'resteasy-code' or 'resteasy-reactive' in the name, plugin vertx will skip");
        return false;
    }

    private String getVersion(ClassInfo info) {
        String version = info.getJarAttribute("Implementation-Version");
        if(version == null) {
            version = info.getJarAttribute("Bundle-Version");
        }
        return version;
    }
}
