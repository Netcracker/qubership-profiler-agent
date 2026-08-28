package com.netcracker.profiler.formatters.title;

import static com.netcracker.profiler.formatters.title.TitleCommonTools.addParameter;

import com.netcracker.profiler.agent.ParameterInfo;

import java.util.List;
import java.util.Map;

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
