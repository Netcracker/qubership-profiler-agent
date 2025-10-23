package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.meta.SuspendHickup;

import java.time.Instant;
import java.util.Map;

// PostgreSQL Table: suspend
public record CloudSuspendEntity(
        Instant date,                       // date [timestamptz], PK
        String podId,                       // pod_id [text]
        String podName,                     // pod_name [text], PK
        Instant restartTime,                // restart_time [timestamptz], PK
        Instant curTime,                    // cur_time [timestamptz], PK
        Map<Integer, Integer> suspendTime   // suspend_time [jsonb]
) {

    public static CloudSuspendEntity prepare(SuspendHickup model) {
        var date = model.truncatedToDays();
        var podId = model.pod().oldPodName();
        var podName = model.pod().podName();
        var restartTime = model.pod().restartTime();
        var curTime = model.truncatedToMinutes();
        Map<Integer, Integer> map = Map.of(model.getSecMs(), model.suspendTime());
        return new CloudSuspendEntity(date, podId, podName, restartTime, curTime, map);
    }
}
