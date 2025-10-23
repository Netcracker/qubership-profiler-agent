package com.netcracker.persistence.adapters.cloud.cdt.dumps;

import java.time.Instant;

public record CloudDumpObjectsEntity(
        String id,              // id or handle [text]
        String podId,           // pod_id [text]
        Instant creationTime,   // creation_time [timestamp]
        Long fileSize,          // file_size [long]
        String dumpType         // dump_type [text]
) {

}
