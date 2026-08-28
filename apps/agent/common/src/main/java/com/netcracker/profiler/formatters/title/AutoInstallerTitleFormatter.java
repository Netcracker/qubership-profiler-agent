package com.netcracker.profiler.formatters.title;

import static com.netcracker.profiler.formatters.title.TitleCommonTools.addParameter;

import com.netcracker.profiler.agent.ParameterInfo;

import java.util.List;
import java.util.Map;

public class AutoInstallerTitleFormatter extends AbstractTitleFormatter {
    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        title.append("AutoInstaller: ");
        title.appendHtml("<b>");
        addParameter(title, tagToIdMap, params, "", "ai.package");
        title.appendHtml("</b>");
        addParameter(title, tagToIdMap, params, ", patch: ", "ai.zip");
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        return formatTitle(classMethod, tagToIdMap, params, null);
    }
}
