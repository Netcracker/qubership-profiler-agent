package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Configuration_01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancerPlugin_undertow23_http extends EnhancerPlugin {

    private final static Logger log = LoggerFactory.getLogger(EnhancerPlugin_undertow23_http.class);

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
    }

    @Override
    public boolean accept(ClassInfo info) {
        String jarName = info.getJarName();
        log.info("Class name: {}, jar name: {}", info.getClassName(), jarName);
        if (jarName == null) {
            return false;
        }

        if (jarName.contains("undertow")) {
            Matcher m = p.matcher(jarName);
            if (!m.find()) {
                return false;
            }

            int majorVersion = Integer.parseInt(m.group(1));
            int minorVersion = Integer.parseInt(m.group(2));
            log.info("Undertow jar version is {}.{}, run undertow23_http plugins = {}",
                    majorVersion, minorVersion, majorVersion >= 2 && minorVersion >= 3);
            return majorVersion >= 2 && minorVersion >= 3;
        }

        log.info("Jar has no 'undertow' in the name, plugin undertow23_http will skip");
        return false;
    }
}
