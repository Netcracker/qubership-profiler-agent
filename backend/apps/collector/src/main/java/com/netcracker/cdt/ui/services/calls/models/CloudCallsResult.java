package com.netcracker.cdt.ui.services.calls.models;

import java.util.List;
import java.util.Set;
import com.netcracker.common.models.pod.PodIdRestart;

public record CloudCallsResult(List<CallRecord> calls, int parsedCalls, int fetchedCalls, Set<PodIdRestart> pods) {
    
}
