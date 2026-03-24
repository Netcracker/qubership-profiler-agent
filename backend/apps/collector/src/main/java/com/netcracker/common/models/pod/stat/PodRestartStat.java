package com.netcracker.common.models.pod.stat;

import com.netcracker.common.models.pod.PodIdRestart;

import java.time.Instant;

public record PodRestartStat(
        PodIdRestart pod,
        Instant curTime,
        PodDataAccumulated accumulated
) {

    public long rotationSize() {
        return accumulated.rotationSize();
    }

    public String getId() {
        return pod.oldPodName() + "-" + curTime().toString().replace(':', '-');
//        return pod.name() + "-" + stream() + "-" + curTime().toString().replace(':', '-');
//        return pod.podName() + "_" + pod.restartTime().toEpochMilli() + "-" + stream() + "-" + curTime().toString().replace(':', '-');
    }
}
