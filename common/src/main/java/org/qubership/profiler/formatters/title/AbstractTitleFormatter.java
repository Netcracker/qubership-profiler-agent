package org.qubership.profiler.formatters.title;

import org.qubership.profiler.agent.ParameterInfo;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

import java.util.*;

public abstract class AbstractTitleFormatter implements ITitleFormatter {

    @Override
    public final ProfilerTitle formatTitle(String classMethod, TIntObjectHashMap<THashSet<String>> params, List<ParameterInfo> defaultListParams) {
        return formatTitle(classMethod, null, params, defaultListParams);
    }

    @Override
    public final ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, List<ParameterInfo> defaultListParams) {
        return formatTitle(classMethod, tagToIdMap, (Object) params, defaultListParams);
    }

    public abstract ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams);
}
