package org.qubership.profiler.formatters.title;

import org.qubership.profiler.agent.ParameterInfo;
import org.qubership.profiler.agent.ProfilerData;

import gnu.trove.THashSet;
import gnu.trove.TIntObjectHashMap;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class TitleCommonTools {
    public static Collection<String> getParameterValues(Map<String, Integer> tagToIdMap, Object params, String name) {
        Collection<String> parameterValues;
        if(tagToIdMap == null) {
            parameterValues = ((TIntObjectHashMap<THashSet<String>>)params).get(ProfilerData.resolveTag(name));
        } else {
            parameterValues = ((Map<Integer, List<String>>)params).get(tagToIdMap.get(name));
        }

        if(parameterValues == null){
            return Collections.EMPTY_LIST;
        }else {
            return parameterValues;
        }
    }

    public static String getParameter(Map<String, Integer> tagToIdMap, Object params, String name) {
        return StringUtils.join(getParameterValues(tagToIdMap, params, name), " ");
    }

    public static boolean addParameter(ProfilerTitleBuilder title, Map<String, Integer> tagToIdMap, Object params, String header, String parameterName, Function<Collection<String>, String> formatFunction) {
        Collection<String> values = getParameterValues(tagToIdMap, params, parameterName);
        if (!values.isEmpty()) {
            title.append(header).append(formatFunction.apply(values));
            return true;
        }
        return false;
    }

    public static boolean addParameter(ProfilerTitleBuilder title, Map<String, Integer> tagToIdMap, Object params, String header, String parameterName) {
        return addParameter(title, tagToIdMap, params, header, parameterName, new Function<Collection<String>, String>() {
            @Override
            public String apply(Collection<String> x) {
                String result = StringUtils.join(x, ", ");
                if(x.size()>1) {
                    result = "["+result+"]";
                }
                return result;
            }
        });
    }

    public static boolean addGenericParams(ProfilerTitleBuilder title, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams, Set<String> skipParams) {
        boolean added = false;
        title.append(" ");
        for(ParameterInfo paramInfo : defaultListParams) {
            if(skipParams.contains(paramInfo.name)) continue;
            if(addParameter(title, tagToIdMap, params, paramInfo.name+": ", paramInfo.name)) {
                added = true;
                title.append(", ");
            }
        }
        if(added) {
            title.deleteLastChars(2);
        }
        return added;
    }
}
