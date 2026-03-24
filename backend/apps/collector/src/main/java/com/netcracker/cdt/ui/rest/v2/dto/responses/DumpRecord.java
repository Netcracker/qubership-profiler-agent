package com.netcracker.cdt.ui.rest.v2.dto.responses;

import com.netcracker.cdt.ui.rest.v2.dto.Responses;
import com.netcracker.persistence.adapters.cloud.cdt.dumps.CloudDumpPodsEntity;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public record DumpRecord(
        String namespace,
        String service,
        String pod,
        long startTime,
        long dataAvailableFrom,
        long dataAvailableTo,
        List<Responses.DownloadOption> downloadOptions
) {
    // Mapping Entity to DTO
    public static DumpRecord of(CloudDumpPodsEntity entity, Instant from, Instant to) {
        return new DumpRecord(
                entity.namespace(),
                entity.serviceName(),
                entity.podName(),
                entity.restartTime().toEpochMilli(),
                entity.restartTime().isBefore(from) ? from.toEpochMilli() : entity.restartTime().toEpochMilli(),
                entity.lastActive().isAfter(to) ? to.toEpochMilli() : entity.lastActive().toEpochMilli(),
                Arrays.stream(entity.dumpType()).filter(Predicate.not("heap"::equals)).map(s -> {
                    String format = "/cdt/v2/download?dateFrom=%d&dateTo=%d&type=%s&namespace=%s&service=%s&podName=%s";
                    String uri = String.format(format, from.toEpochMilli(), to.toEpochMilli(), s, entity.namespace(),
                            entity.serviceName(), entity.podName());
                    return new Responses.DownloadOption(s, uri);
                }).toList()
        );
    }
}
