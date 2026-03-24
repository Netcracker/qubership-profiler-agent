package com.netcracker.cdt.collector.common.models;


import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;

import java.time.Instant;

public record StreamInfoRequest(PodIdRestart id,
                                StreamType stream,
                                int requestedRollingSequenceId,
                                boolean resetRequired,
                                boolean forceRequestedRollingSequenceId,
                                Instant createdWhen,
                                Instant modifiedWhen
) {

    public static StreamInfoRequest of(String originalPodName, StreamType stream,
                                       int requestedRollingSequenceId, boolean resetRequired,
                                       boolean forceRequestedRollingSequenceId,
                                       Instant createdWhen, Instant modifiedWhen
                                       ) {
        return new StreamInfoRequest(PodIdRestart.of(originalPodName), stream,
                requestedRollingSequenceId, resetRequired, forceRequestedRollingSequenceId, createdWhen, modifiedWhen);
    }

    public static StreamInfoRequest of(PodInfo podInfo, StreamType stream,
                                       int requestedRollingSequenceId, boolean resetRequired,
                                       boolean forceRequestedRollingSequenceId,
                                       Instant createdWhen, Instant modifiedWhen
                                       ) {
        return new StreamInfoRequest(podInfo.restartId(), stream,
                requestedRollingSequenceId, resetRequired, forceRequestedRollingSequenceId, createdWhen, modifiedWhen);
    }

    public String podId() {
        return id.podId();
    }

    @Override
    public String toString() {
        return String.format("StreamReq(%s[%s]: %d, %b, %b)",
                id.oldPodName(), stream.getName(),
                requestedRollingSequenceId, resetRequired, forceRequestedRollingSequenceId);
    }
}
