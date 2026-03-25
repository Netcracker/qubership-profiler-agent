package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.stat.PodDataAccumulated;
import com.netcracker.common.models.pod.stat.PodRestartStat;

import java.time.Instant;
import java.util.Map;

// PostgreSQL Table: pod_statistics
public record CloudPodStatisticsEntity(
        Instant date,                           // date [timestamptz], PK
        String podId,                           // pod_id [text]
        String podName,                         // pod_name [text], PK
        Instant restartTime,                    // restart_time [timestamptz], PK
        Instant curTime,                        // cur_time [timestamptz], PK
        Map<String, Long> dataAccumulated,      // data_accumulated [jsonb] // zipped
        Map<String, Long> originalAccumulated   // original_accumulated [jsonb] // original
) {

    public static CloudPodStatisticsEntity prepare(Instant date, Instant curMinute, PodIdRestart pod, PodDataAccumulated accumulated) {
        var podId = pod.oldPodName();
        var podName = pod.podName();
        var restartTime = pod.restartTime();
        var dataAccumulated = accumulated.forDb(false);
        var originalAccumulated = accumulated.forDb(true);
        return new CloudPodStatisticsEntity(date, podId, podName, restartTime, curMinute, dataAccumulated, originalAccumulated);
    }

    public PodRestartStat toModel(PodIdRestart pod) {
        return new PodRestartStat(pod, curTime(),
                PodDataAccumulated.fromDb(originalAccumulated(), dataAccumulated()));
    }
}
