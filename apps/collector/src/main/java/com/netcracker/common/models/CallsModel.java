package com.netcracker.common.models;

import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.profiler.model.Call;

import java.util.List;
import java.util.Map;

public record CallsModel(
       long time,
       long cpuTime,
       long waitTime,
       long memoryUsed,
       int method,
       int duration,
       long nonBlocking,
       int queueWaitDuration,
       int suspendDuration,
       int calls,
       int traceFileIndex,
       int bufferOffset,
       int recordIndex,
       long transactions,
       int logsGenerated,
       int logsWritten,
       long fileRead,
       long fileWritten,
       long netRead,
       long netWritten,
       String threadName,
       String callsStreamIndex,
       Map<Integer, List<String>> params,
       PodIdRestart podIdRestart
) {

    public static CallsModel of(Call call, PodIdRestart podIdRestart) {
        return new CallsModel(
                call.time,
                call.cpuTime,
                call.waitTime,
                call.memoryUsed,
                call.method,
                call.duration,
                call.nonBlocking,
                call.queueWaitDuration,
                call.suspendDuration,
                call.calls,
                call.traceFileIndex,
                call.bufferOffset,
                call.recordIndex,
                call.transactions,
                call.logsGenerated,
                call.logsWritten,
                call.fileRead,
                call.fileWritten,
                call.netRead,
                call.netWritten,
                call.threadName,
                call.callsStreamIndex,
                call.params,
                podIdRestart
        );
    }
}
