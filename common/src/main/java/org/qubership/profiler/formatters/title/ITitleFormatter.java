package org.qubership.profiler.formatters.title;

import org.qubership.profiler.agent.ParameterInfo;

import gnu.trove.THashSet;
import gnu.trove.TIntObjectHashMap;

import java.util.List;
import java.util.Map;

public interface ITitleFormatter {

    ProfilerTitle formatTitle(String classMethod, TIntObjectHashMap<THashSet<String>> params, List<ParameterInfo> defaultListParams);
    ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, List<ParameterInfo> defaultListParams);
    ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext);

}
