package com.netcracker.profiler.formatters.title;

import static com.netcracker.profiler.formatters.title.TitleCommonTools.addGenericParams;
import static com.netcracker.profiler.formatters.title.TitleCommonTools.addParameter;

import com.netcracker.profiler.agent.ParameterInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OrchestratorTitleFormatter extends AbstractTitleFormatter {

    private Set<String> SKIP_PARAMS = Collections.EMPTY_SET;

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        title.append("Orchestrator: ");
        title.appendHtml("<b>");
        addParameter(title, tagToIdMap, params, "", "po.process.name");
        title.appendHtml("/<b>");
        addGenericParams(title, tagToIdMap, params, defaultListParams, SKIP_PARAMS);
        addParameter(title, tagToIdMap, params, ", text: ", "jms.text.fragment");
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        throw new UnsupportedOperationException();
    }
}
