package com.netcracker.cdt.ui.services.calls;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import joptsimple.internal.Strings;

import java.util.List;
import java.util.function.Function;

public record CallsListResult(
        String displayHash,
        Status status,
        List<CallRecord> calls
) {

    public record Status(boolean finished, int progress,
                         List<String> exceptions,
                         long filteredRecords, long processedRecords,
                         int foundPods, int successfulRequests, int timedOutRequests, int failedRequests
                         ) {

        public String error() {
            return Strings.join(this.exceptions(), "\n");
        }

    }

    public static CallsListResult.Status emptyStatus() {
        return new CallsListResult.Status(false, 0, List.of(), 0, 0, 0, 0, 0, 0);
    }

    public <R> List<R> convertCalls(Function<CallRecord, R> mapper) {
        return calls.stream().map(mapper).toList();
    }

}
