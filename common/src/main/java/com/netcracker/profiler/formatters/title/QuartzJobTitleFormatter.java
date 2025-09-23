package com.netcracker.profiler.formatters.title;

import static com.netcracker.profiler.formatters.title.TitleCommonTools.*;

import com.netcracker.profiler.agent.ParameterInfo;

import org.apache.commons.lang.StringUtils;

import java.util.*;

public class QuartzJobTitleFormatter extends AbstractTitleFormatter {

    private Set<String> SKIP_PARAMS = Collections.EMPTY_SET;

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        String jobType = getParameter(tagToIdMap, params, "job.action.type");
        title.append(getJobTypeNameById(jobType));

        String jobName = getParameter(tagToIdMap, params, "job.name");
        title.append(" ").appendHtml("<b>").append(jobName.isEmpty() ? "Name unknown" : jobName).appendHtml("</b>");
        addParameter(title, tagToIdMap, params, " ", "job.id", new Function<Collection<String>, String>() {
            @Override
            public String apply(Collection<String> jobIDs) {
                return "(" + StringUtils.join(jobIDs, ",") + ")";
            }
        });
        if ("7020873015013388042".equals(jobType)) {
            String jobClass = getParameter(tagToIdMap, params, "job.class");
            String jobMethod = getParameter(tagToIdMap, params, "job.method");
            title.append(" (").append(getClassName(jobClass)).append(".").append(jobMethod).append(")");
        }

        addGenericParams(title, tagToIdMap, params, defaultListParams, SKIP_PARAMS);
        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        String jobType = getParameter(tagToIdMap, params, "job.action.type");
        title.append(getJobTypeNameById(jobType));

        String jobName = getParameter(tagToIdMap, params, "job.name");
        title.append(" ").append(jobName.isEmpty() ? "Name unknown" : jobName);
        if ("7020873015013388042".equals(jobType)) {
            String jobClass = getParameter(tagToIdMap, params, "job.class");
            String jobMethod = getParameter(tagToIdMap, params, "job.method");
            title.append(" (").append(getClassName(jobClass)).append(".").append(jobMethod).append(")");
        }

        return title;
    }

    private static String getJobTypeNameById(String jobTypeId) {
        switch (jobTypeId) {
            case "7020873015013388039":
                return "JMS quartz job";
            case "7020873015013388040":
                return "URL quartz job";
            case "7020873015013388041":
                return "EJB quartz job";
            case "7020873015013388042":
                return "Class quartz job";
            case "7020873015013388043":
                return "SOAP quartz job";
            default:
                if (jobTypeId.isEmpty()) {
                    return "Quartz job";
                } else {
                    return jobTypeId + " quartz job";
                }
        }
    }

    private static String getClassName(String fullClassName) {
        int dotIndex = fullClassName.lastIndexOf('.');
        return dotIndex == -1 ? fullClassName : fullClassName.substring(dotIndex + 1);
    }
}
