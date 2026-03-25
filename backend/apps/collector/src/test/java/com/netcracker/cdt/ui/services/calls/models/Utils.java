package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.common.models.dict.ParameterValue;
import com.netcracker.common.models.meta.dict.CallParameters;
import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.profiler.model.Call;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Utils {

    public static CallRecord callRecord(Instant t,
                                 int calls, int duration, long cpuTime,
                                 String method, ParameterValue... pvs) {
        var pod = PodIdRestart.of("test_1234534");

        return new CallRecord(t.toEpochMilli(),
                duration, 100, cpuTime, 1, 2,
                calls, pod, "traceRecordId", method, 12,
                1234000, 10000, 7000, 10, 7, 200000, 198000,
                params(pvs));
    }

    public static CallParameters params(ParameterValue... pvs) {
        var params = new CallParameters(new HashMap<>());
        for (var pv: pvs) {
            params.put(pv.name(), pv.values());
        }
        return params;
    }

    public static ParameterValue epv(String name) {
        return new ParameterValue(name, List.of());
    }

    public static ParameterValue pv(String name, Integer... valueIds) {
        var list = Stream.of(valueIds).map(n -> Integer.toString(n)).toList();
        return new ParameterValue(name, list);
    }

    public static ParameterValue pv(String name, String... values) {
        return new ParameterValue(name, Arrays.asList(values));
    }

    public static PodMetaData pod(String podName) {
        return PodMetaData.empty(PodInfo.empty(podName, Instant.EPOCH.toEpochMilli()));
    }

    public static Call originCall(Instant t,
                           int duration,
                           long nonBlocking,
                           long cpuTime,
                           int queueWaitDuration,
                           int suspendDuration,
                           int calls,
                           int traceFileIndex, int bufferOffset, int recordIndex,
                           int method,
                           long transactions,
                           long memoryUsed,
                           int logsGenerated, int logsWritten,
                           long fileRead, long fileWritten,
                           long netRead, long netWritten,
                           Map<Integer, List<String>> params
    ) {
        var c = new Call();
        c.time = t.toEpochMilli();
        c.duration = duration;
        c.nonBlocking = nonBlocking;
        c.cpuTime = cpuTime;
        c.queueWaitDuration = queueWaitDuration;
        c.suspendDuration = suspendDuration;
        c.calls = calls;
        c.traceFileIndex = traceFileIndex;
        c.bufferOffset = bufferOffset;
        c.recordIndex = recordIndex;
        c.method = method;
        c.transactions = transactions;
        c.memoryUsed = memoryUsed;
        c.logsGenerated = logsGenerated;
        c.logsWritten = logsWritten;
        c.fileRead = fileRead;
        c.fileWritten = fileWritten;
        c.netRead = netRead;
        c.netWritten = netWritten;
        c.params = params;
        return c;
    }

}
