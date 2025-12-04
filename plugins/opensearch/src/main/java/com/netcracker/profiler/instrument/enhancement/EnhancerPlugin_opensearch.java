package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancerPlugin_opensearch extends EnhancerPlugin {

    private final static Logger log = LoggerFactory.getLogger(EnhancerPlugin_opensearch.class);

    private static final Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);

        configuration.getParameterInfo("os.request.type").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.indices").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.source.size").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.source.query").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.id").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.index").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.count").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.size").big(true).deduplicate(true);
        configuration.getParameterInfo("os.request.scrollId").big(true).deduplicate(true);

        configuration.getParameterInfo("os.response.took").big(true).deduplicate(true);
        configuration.getParameterInfo("os.response.hits").big(true).deduplicate(true);
        configuration.getParameterInfo("os.response.scrollId").big(true).deduplicate(true);
        configuration.getParameterInfo("os.response.pointInTimeId").big(true).deduplicate(true);
        configuration.getParameterInfo("os.response.isTimeout").big(true).deduplicate(true);
        configuration.getParameterInfo("os.response").big(true).deduplicate(true);
    }

    @Override
    public boolean accept(ClassInfo info) {

        String jarName = info.getJarName();
        log.info("Class name: {}, jar name: {}", info.getClassName(), jarName);
        if (jarName == null) {
            return false;
        }

        Matcher m = p.matcher(jarName);
        if (!m.find()) {
            return false;
        }

        int majorVersion = Integer.parseInt(m.group(1));
        return majorVersion == 2;
    }
}
