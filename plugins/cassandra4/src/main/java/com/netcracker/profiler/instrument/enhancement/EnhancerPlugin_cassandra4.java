package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class EnhancerPlugin_cassandra4 extends EnhancerPlugin {

    private final static Logger log = LoggerFactory.getLogger(EnhancerPlugin_cassandra4.class);

    private static final Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    @Override
    public void init(Element e, Configuration_01 configuration) {
        super.init(e, configuration);
        configuration.getParameterInfo("binds").big(true);
        configuration.getParameterInfo("sql").big(true).deduplicate(true);
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
        return majorVersion == 4;
    }
}
