package com.netcracker.common.models.pod;

import static java.time.temporal.ChronoUnit.DAYS;
import java.time.Instant;
import java.util.UUID;

public record Dumps(
        String id,
        UUID podId,
        Instant creationTime,
        Long fileSize,
        String dumpType

) {
    public String getPrimaryKey() {
        return id;
    }

    public Instant day() {
        return creationTime.truncatedTo(DAYS);
    }
}
