package com.netcracker.persistence.adapters.cloud.cdt;

import com.netcracker.common.models.TimeRange;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;

// PostgreSQL Table: pod_restarts
public record CloudPodRestartsEntity(
        String podId,           // pod_id [text]
        String namespace,       // namespace [text], PK
        String serviceName,     // service_name [text], PK
        String podName,         // pod_name [text], PK
        Instant restartTime,    // restart_time [timestamptz], PK
        Instant activeSince,    // active_since [timestamptz]
        Instant lastActive      // last_active [timestamptz]
) {

    public boolean isValid() {
        return !StringUtils.isBlank(podName) && activeSince.isBefore(lastActive);
    }

    public TimeRange range() {
        return TimeRange.of(activeSince, lastActive);
    }

    public boolean wasActive(Instant rangeFrom, Instant rangeTo) {
        if (rangeTo.isBefore(activeSince)) return false;
        return !rangeFrom.isAfter(lastActive);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CloudPodRestartsEntity{");
        sb.append("podId='").append(podId).append('\'');
        sb.append(", namespace='").append(namespace).append('\'');
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", podName='").append(podName).append('\'');
        sb.append(", restartTime=").append(restartTime);
        sb.append(", activeSince=").append(activeSince);
        sb.append(", lastActive=").append(lastActive);
        sb.append('}');
        return sb.toString();
    }
}
