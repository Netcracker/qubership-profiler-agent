package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.pod.PodInfo;

import java.time.Instant;
import java.util.Map;

// PostgreSQL Table: pods
public record CloudPodsEntity(
        String podId,               // pod_id [text]
        String namespace,           // namespace [text], PK
        String serviceName,         // service_name [text], PK
        String podName,             // pod_name [text], PK
        Instant activeSince,        // active_since [timestamp]
        Instant lastRestart,        // last_restart [timestamp]
        Instant lastActive,         // last_active [timestamp]
        Map<String, String> tags    // tags [jsonb]
) {
    public PodInfo asPodInfo() {
        return PodInfo.ofDb(namespace, serviceName, podName, podId, activeSince, lastRestart, lastActive, tags);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CloudPodsEntity{");
        sb.append("podId='").append(podId).append('\'');
        sb.append(", namespace='").append(namespace).append('\'');
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", podName='").append(podName).append('\'');
        sb.append(", activeSince=").append(activeSince);
        sb.append(", lastRestart=").append(lastRestart);
        sb.append(", lastActive=").append(lastActive);
        sb.append(", tags=").append(tags);
        sb.append('}');
        return sb.toString();
    }
}
