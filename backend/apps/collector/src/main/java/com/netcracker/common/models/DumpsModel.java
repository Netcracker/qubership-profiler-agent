package com.netcracker.common.models;

import com.netcracker.common.models.pod.PodInfo;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public record DumpsModel(

        PodInfo podInfo,
        StreamType stream,
        Instant createdTime,
        InputStream dumpFile,
        UUID uuid,
        Long bytesSize
) {

    public static DumpsModel of(PodInfo podInfo, StreamType stream, Instant createdTime, InputStream dumpFile, UUID uuid, Long bytesSize) {
        return new DumpsModel(podInfo, stream, createdTime, dumpFile, uuid, bytesSize);
    }

    public String namespace() {
        return podInfo.namespace();
    }

    public String serviceName() {
        return podInfo.service();
    }

    public String podName() {
        return podInfo.podName();
    }

    public String podType() {
        // TODO: fill actual data
        return "podType";
    }

    public Instant restartTime() {
        return podInfo.restartTime();
    }

    public String type() {
        return stream.name();
    }

    public Map<String, String> info() {
        // TODO: fill actual data
        var info = new HashMap<String, String>();
        info.put("key1", "value1");
        info.put("key2", "value2");
        return info;
    }

    public InputStream binaryData() {
        return dumpFile;
    }

}
