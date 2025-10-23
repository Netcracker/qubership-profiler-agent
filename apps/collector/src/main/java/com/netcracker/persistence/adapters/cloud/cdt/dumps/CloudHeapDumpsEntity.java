package com.netcracker.persistence.adapters.cloud.cdt.dumps;

import java.time.Instant;

public record CloudHeapDumpsEntity(
        String namespace,       // namespace [text]
        String serviceName,     // service_name [text]
        String podName,         // pod_name [text]
        String handle,          // handle [text]
        Instant creationTime,   // creation_time [timestamp]
        Long fileSize           // file_size [integer]
) {

}
