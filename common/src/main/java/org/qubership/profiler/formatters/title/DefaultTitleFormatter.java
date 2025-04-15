package org.qubership.profiler.formatters.title;

import static org.qubership.profiler.formatters.title.TitleCommonTools.addGenericParams;

import org.qubership.profiler.agent.ParameterInfo;

import java.util.*;

public class DefaultTitleFormatter extends AbstractTitleFormatter {

    private Set<String> SKIP_PARAMS = new HashSet<>(Arrays.asList("j2ee.xid", "common.started", "java.thread"));

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder(classMethod);
        title.setDefault(true);
        addGenericParams(title, tagToIdMap, params, defaultListParams, SKIP_PARAMS);
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder(classMethod);
        title.setDefault(true);
        return title;
    }
}
