package com.netcracker.persistence.adapters.cloud.cdt.dumps;

import java.time.Instant;

public record CloudDumpsEntity(
        String podName,             // pod_name [text]
        Instant startTime,          // start_time [timestamp]
        String dumpType,            // dump_type [dump_object_type]
        Instant dataAvailableFrom,  // creation_time [timestamp]
        Instant dataAvailableTo     // creation_time [timestamp]
) {

}
