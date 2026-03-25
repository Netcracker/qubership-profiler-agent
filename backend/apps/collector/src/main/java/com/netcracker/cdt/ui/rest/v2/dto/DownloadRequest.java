package com.netcracker.cdt.ui.rest.v2.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.netcracker.common.models.pod.IPodFilter;

import java.time.Instant;

public record DownloadRequest(
//        https://www.baeldung.com/jackson-serialize-dates
        @JsonFormat(without = {JsonFormat.Feature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, JsonFormat.Feature.WRITE_DATES_WITH_ZONE_ID})
        Instant from,
        @JsonFormat(without = {JsonFormat.Feature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, JsonFormat.Feature.WRITE_DATES_WITH_ZONE_ID})
        Instant to,
        String namespace,
        String service) {

        public IPodFilter getFilter() {
                return p -> {
                        if (namespace == null || !namespace.equals(p.namespace())) return false;
                        if (service != null && !service.equals(p.service())) return false;
                        if (!p.wasActive(this.from, this.to)) return false;
                        return true;
                };
        }
}
