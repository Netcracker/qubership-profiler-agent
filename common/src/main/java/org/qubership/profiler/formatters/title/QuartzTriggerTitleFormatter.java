package org.qubership.profiler.formatters.title;

import org.qubership.profiler.agent.ParameterInfo;

import java.util.List;
import java.util.Map;

public class QuartzTriggerTitleFormatter extends AbstractTitleFormatter {
    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        return new ProfilerTitleBuilder("Quartz trigger");
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        return formatTitle(classMethod, tagToIdMap, params, null);
    }
}
