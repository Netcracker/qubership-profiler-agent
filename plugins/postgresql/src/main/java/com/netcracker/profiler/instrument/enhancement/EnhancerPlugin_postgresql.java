package com.netcracker.profiler.instrument.enhancement;

import com.netcracker.profiler.agent.Configuration_01;

import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancerPlugin_postgresql extends EnhancerPlugin {
    @Override
    public void init(Element node, Configuration_01 conf) {
        super.init(node, conf);
        conf.getParameterInfo("sql").big(true).deduplicate(true);
        conf.getParameterInfo("binds").big(true);
        conf.getParameterInfo("pg_application_name").index(true);
    }

    @Override
    public boolean accept(ClassInfo info) {
        String version = info.getJarAttribute("Implementation-Version");
        if (version == null) {
            return false;
        }

        Pattern p = Pattern.compile("(\\d+)\\.(\\d+)");
        Matcher m = p.matcher(version);

        if (!m.find()) {
            return false;
        }

        int majorVersion = Integer.parseInt(m.group(1));
        int minorVersion = Integer.parseInt(m.group(2));

        return (majorVersion == 9 && minorVersion >=4) || majorVersion > 9;
    }
}
