package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.common.models.meta.dict.Parameter;
import com.netcracker.common.models.meta.dict.CallParameters;
import com.netcracker.profiler.model.Call;
import io.quarkus.logging.Log;

import java.util.*;
import java.util.stream.Stream;

public record CallConverter(PodMetaData podInfo, Set<Integer> missedTags) {

    public static CallConverter create(PodMetaData podInfo) {
        var missedTags = new TreeSet<Integer>();
        return new CallConverter(podInfo, missedTags);
    }

    public void debug() {
        if (Log.isDebugEnabled()) {
            Log.debugf("[%s] %d method tags missed, %d tags available",
                    podInfo.oldPodName(), missedTags.size(), podInfo.tagsSize());
        }
    }

    public static Stream<CallRecord> convert(Stream<Call> stream, PodMetaData podInfo) {
        var converter = CallConverter.create(podInfo);
        return stream.
                map(converter::convertCall).
                filter(Objects::nonNull);
    }

    public CallRecord convertCall(Call o) {
        String method = podInfo.getLiteral(o.method);
        if (method == null) {
            if (Log.isDebugEnabled()) {
                missedTags.add(o.method);
            }
            String msg = String.format("No tag[method:%d] for pod %s, %d tags available. Skip", o.method, podInfo.oldPodName(), podInfo.tagsSize());
//            throw new IllegalStateException(msg);
            Log.tracef(msg);
            return null;
        }

        String traceRecordId = String.format("%d_%d_%d", o.traceFileIndex, o.bufferOffset, o.recordIndex);

        CallParameters params = new CallParameters(new HashMap<>());
        if (o.params != null) {
            o.params.forEach((k, v) -> {
                var param = podInfo.getParameter(k); // find by id
                if (param != null) {
                    if (!v.isEmpty()) {
                        params.put(param.name(), v);
                    }
                }
            });
        }

        if (o.threadName != null) {
            params.put(Parameter.THREAD_NAME.name(), List.of(o.threadName));
        }

        return new CallRecord(o.time, o.duration,
                o.nonBlocking, o.cpuTime, o.queueWaitDuration, o.suspendDuration,
                o.calls,
                podInfo.podId(),
                traceRecordId, method,
                o.transactions,
                o.memoryUsed, o.logsGenerated, o.logsWritten, o.fileRead, o.fileWritten, o.netRead, o.netWritten,
                params);
    }
}
