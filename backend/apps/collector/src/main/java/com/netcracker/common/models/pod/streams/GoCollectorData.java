package com.netcracker.common.models.pod.streams;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

public record GoCollectorData(

        @JsonProperty("namespace")
        String namespace,

        @JsonProperty("service")
        String service,

        @JsonProperty("pod_name")
        String podName,

        @JsonProperty("container_name")
        String containerName,

        @JsonProperty("profile_type")
        String profileType,

        @JsonProperty("labels")
        List<Object> labels,

        @JsonProperty("collect_time")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'hh:mm:ss")
        Instant collectTime,

        @JsonProperty("profile")
        byte[] profile,

        @JsonProperty("data_accumulated")
        int dataAccumulated
) {
}
