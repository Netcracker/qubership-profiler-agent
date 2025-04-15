package org.qubership.profiler.configuration.callfilters.params;

import org.qubership.profiler.agent.FilterOperator;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.dump.ThreadState;

import gnu.trove.THashSet;
import gnu.trove.TIntObjectHashMap;

import java.util.Map;

public abstract class FilterOperatorMatching implements FilterOperator {

    protected String name;
    protected String value;

    FilterOperatorMatching(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public boolean evaluate(Map<String, Object> params) {
        for (String parameterValue : getThreadStateParameter(params, name)) {
            if (evaluateCondition(parameterValue)) {
                return true;
            }
        }

        Map<String, String> additionalInputParams = (Map<String, String>) params.get(ADDITIONAL_INPUT_PARAMS);
        String additionalParameterValue = additionalInputParams.get(name);
        if(additionalParameterValue != null && evaluateCondition(additionalParameterValue)) {
            return true;
        }

        return false;
    }

    abstract boolean evaluateCondition(String actualValue);

    private THashSet<String> getThreadStateParameter(Map<String, Object> params, String parameterName) {
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);
        TIntObjectHashMap<THashSet<String>> threadStateParams = threadState.params;
        if(threadStateParams == null) {
            return new THashSet<>();
        }
        THashSet<String> parameterValues = threadStateParams.get(ProfilerData.resolveTag(parameterName));
        if (parameterValues == null) {
            parameterValues = new THashSet<>();
        }

        return parameterValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FilterOperatorMatching that = (FilterOperatorMatching) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
