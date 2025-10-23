package com.netcracker.cdt.ui.services.calls.view;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.common.search.filter.FilterCondition;
import com.netcracker.common.search.filter.FilterParser;
import com.netcracker.profiler.model.CallFilterer;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.function.BiFunction;

public class UiCallRecordFilter implements CallFilterer<CallRecord> {

    private final boolean hideSystem;
    private final FilterCondition condition;

    UiCallRecordFilter(String filterString, boolean hideSystem) {
        this.hideSystem = hideSystem;
        this.condition = FilterParser.parse(filterString);
    }

    public static UiCallRecordFilter create(String query) {
        return new UiCallRecordFilter(query, false);
    }

    @Override
    public boolean filter(CallRecord call) {
        FilterCondition.Matcher res = condition.start(true);

        if (hideSystem) {
            if (call.isIdleMethod()) return false;
        }
        if (res.addGeneralString(call.method())) {
            return false;
        }
        if (!iterateParams(res::addParameterValuesByName, call)) {
            return false;
        }
        return res.matches();
    }

    public boolean iterateParams(BiFunction<String, List<String>, Boolean> test, CallRecord call) {
        var params = call.params();
        if (hideSystem) {
            if (params.isSystem()) return true;
        }

        for (var tag : params.names()) {
            if (StringUtils.isEmpty(tag)) {
//                Log.errorf("*** invalid tag: %v", tag);
                continue;
            }
            List<String> values = params.values(tag);
            if (test.apply(tag, values)) {
                return false;
            }
        }
        return true;
    }

}
