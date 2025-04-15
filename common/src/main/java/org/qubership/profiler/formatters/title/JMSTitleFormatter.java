package org.qubership.profiler.formatters.title;

import static org.qubership.profiler.formatters.title.TitleCommonTools.*;

import org.qubership.profiler.agent.ParameterInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JMSTitleFormatter extends AbstractTitleFormatter {

    private Set<String> SKIP_PARAMS = Collections.EMPTY_SET;

    private OrchestratorTitleFormatter orchestratorFormatter = new OrchestratorTitleFormatter();

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        String consumer = getParameter(tagToIdMap, params, "jms.consumer");
        if ("OrchestrationQueueInvokerBean".equals(consumer)) {
            return orchestratorFormatter.formatTitle(classMethod, tagToIdMap, params, defaultListParams);
        }
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        title.append("JMS: ").appendHtml("<b>").append(consumer).appendHtml("</b>");
        addGenericParams(title, tagToIdMap, params, defaultListParams, SKIP_PARAMS);
        addParameter(title, tagToIdMap, params, ", destination: ", "jms.destination");
        addParameter(title, tagToIdMap, params, ", text: ", "jms.text.fragment");
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        String consumer = getParameter(tagToIdMap, params, "jms.consumer");
        title.append("JMS: ").append(consumer);
        return title;
    }
}
