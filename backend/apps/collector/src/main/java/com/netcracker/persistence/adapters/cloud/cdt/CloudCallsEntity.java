package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.CallsModel;

import java.time.Instant;
import java.util.List;
import java.util.Map;

// PostgreSQL Table: calls_%timestamp%
public record CloudCallsEntity (
        Instant time,               // time [timestamptz] -- time when func/method was called
        Long cpuTime,               // cpu_time [bigint]
        Long waitTime,              // wait_time [bigint]
        Long memoryUsed,            // memory_used [bigint]
        Long duration,              // duration [bigint]
        Long nonBlocking,           // non_blocking [bigint]
        Integer queueWaitDuration,  // queue_wait_duration [integer]
        Integer suspendDuration,    // suspend_duration [integer]
        Integer calls,              // calls [integer]
        Long transactions,          // transactions [bigint]
        Integer logsGenerated,      // logs_generated [integer]
        Integer logsWritten,        // logs_written [integer]
        Long fileRead,              // file_read [bigint]
        Long fileWritten,           // file_written [bigint]
        Long netRead,               // net_read [bigint]
        Long netWritten,            // net_written [bigint]
        String namespace,           // namespace [text]
        String serviceName,         // service_name [text]
        String podName,             // pod_name [text]
        Instant restartTime,        // restart_time [timestamptz]
        Integer method,             // method [integer]
        Map<Integer, List<String>> params, // params [jsonb]
        Integer traceFileIndex,     // trace_file_index [integer]
        Integer bufferOffset,       // buffer_offset [integer]
        Integer recordIndex         // record_index [integer]
) {

    public static CloudCallsEntity prepare(CallsModel model) {
        return new CloudCallsEntity(
                Instant.ofEpochMilli(model.time()),
                model.cpuTime(),
                model.waitTime(),
                model.memoryUsed(),
                (long) model.duration(), // TODO: why duration is integer in model but long in DB
                model.nonBlocking(),
                model.queueWaitDuration(),
                model.suspendDuration(),
                model.calls(),
                model.transactions(),
                model.logsGenerated(),
                model.logsWritten(),
                model.fileRead(),
                model.fileWritten(),
                model.netRead(),
                model.netWritten(),
                model.podIdRestart().namespace(),
                model.podIdRestart().service(),
                model.podIdRestart().podName(),
                model.podIdRestart().restartTime(),
                model.method(),
                model.params(),
                model.traceFileIndex(),
                model.bufferOffset(),
                model.recordIndex()
        );
    }

}
