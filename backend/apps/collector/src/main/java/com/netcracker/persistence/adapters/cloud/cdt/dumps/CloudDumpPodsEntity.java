package com.netcracker.persistence.adapters.cloud.cdt.dumps;

import java.time.Instant;

public record CloudDumpPodsEntity(
        String id,              // id [text] (e.g. 01961fba-8ff4-7987-ae9a-b5f584c54150)
        String namespace,       // namespace [text]
        String serviceName,     // service_name [text]
        String podName,         // pod_name [text] (e.g. quarkus-3-vertx-685456586c-2h596_1743109309237)
        Instant restartTime,    // restart_time [timestamp]
        Instant lastActive,     // last_active [timestamp]
        String[] dumpType       // dump_type [dump_object_type[]]
) {

}
