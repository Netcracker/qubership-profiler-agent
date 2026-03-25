package com.netcracker.persistence.adapters.cloud.cdt;

import java.time.Instant;

// Moved to CloudDumpsObjectEntity
public record CloudDumpEntity(
        String id,               // id or handle [text]
        String podId,              // pod_id [text], PK
        Instant creationTime,    // creation_time [timestamp]
        Long fileSize,           // file_size [long]
        String dumpType          // dump_type [text]
) {


}