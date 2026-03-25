package com.netcracker.cdt.ui.services.calls.search;

import com.netcracker.common.models.DurationRange;
import com.netcracker.common.search.filter.FilterCondition;
import com.netcracker.common.search.filter.FilterParser;
import com.netcracker.profiler.model.Call;
import com.netcracker.profiler.model.CallFilterer;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Internal filter for `Call` entities.
 * <br>
 * Filter by duration and parameters values (tags for method name not yet loaded)
 * <br>
 * Goal: exclude at early stages calls which doesn't fit criteria
 */
public class InternalCallFilter implements CallFilterer<Call> {

    private final DurationRange range;
    private final FilterCondition condition;

    public InternalCallFilter(DurationRange range) {
        this(range, "");
    }

    public InternalCallFilter(DurationRange range, String filterString) {
        this.range = range;
        this.condition = FilterParser.parse(filterString);
    }

    private InternalCallFilter(DurationRange range, FilterCondition condition) {
        this.range = range;
        this.condition = condition;
    }

    @Override
    public boolean filter(Call call) {
        // goal: exclude at early stages calls which doesn't fit criteria

        if (!range.inRange(call.duration)) { // by duration
            return false;
        }
        if (condition.isEmpty()) { // no query
            return true;
        }

        // by query if available
        var res = condition.start(false);
        if (!iterateParams(res::addParameterValuesById, call)) {
            return false;
        }
        // could not check against method and parameter names (not yet provided, we have only their ids)

        return res.matches();
    }

    public boolean iterateParams(BiFunction<Integer, List<String>, Boolean> addParameterValues, Call call) {
        if (call.params == null) {
            return !condition.hasMandatoryParams();
        }

        for (var e : call.params.entrySet()) {
            Integer parameterId = e.getKey();
            List<String> values = e.getValue();
            final var wontMatchAnyway = addParameterValues.apply(parameterId, values);
            if (wontMatchAnyway) {
                return false;
            }
        }
        return true;
    }

    public InternalCallFilter enrich(Map<String, Integer> paramToIdMapping) {
        return new InternalCallFilter(range, condition.copyWithPopulatedIds(paramToIdMapping));
    }

}
