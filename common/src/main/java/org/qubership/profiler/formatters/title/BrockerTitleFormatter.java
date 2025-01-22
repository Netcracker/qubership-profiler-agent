package org.qubership.profiler.formatters.title;

import org.qubership.profiler.agent.ParameterInfo;
import gnu.trove.THashSet;
import gnu.trove.TIntObjectHashMap;

import java.util.List;
import java.util.Map;

import static org.qubership.profiler.formatters.title.TitleCommonTools.addParameter;

public class BrockerTitleFormatter extends AbstractTitleFormatter {

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        title.appendHtml("<b>").append("RabbitMQ Url: ").appendHtml("</b>");
        addParameter(title, tagToIdMap, params, "Url: ", "rabbitmq.url");
        addParameter(title, tagToIdMap, params, ", queue: ", "queue");
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        return formatTitle(classMethod, tagToIdMap, params, null);
    }
}
