package org.qubership.profiler.instrument.enhancement;

import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.PropertyFacadeBoot;
import org.qubership.profiler.util.NaturalComparator;
import org.w3c.dom.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancerPlugin_ant extends EnhancerPlugin {
    private static final String JAR_MANIFEST_ENTRY_NAME = "org/apache/tools/ant/";

    @Override
    public void init(Element node, Configuration_01 conf) {
        super.init(node, conf);
        conf.getParameterInfo("ai.zip").index(true);
        conf.getParameterInfo("ai.package").index(true);
        conf.getParameterInfo("command.line").big(false);
        conf.getParameterInfo("antcall.json").big(false);
    }

    @Override
    public boolean accept(ClassInfo info) {
        String version = info.getJarSubAttribute(JAR_MANIFEST_ENTRY_NAME, "Implementation-Version");
        return version != null && NaturalComparator.INSTANCE.compare(version, "1.10.1") <= 0;
    }
}
