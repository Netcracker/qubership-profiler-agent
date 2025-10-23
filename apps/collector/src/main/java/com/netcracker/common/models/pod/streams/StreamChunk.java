package com.netcracker.common.models.pod.streams;

import com.netcracker.persistence.utils.ByteBufferInputStream;

import java.io.InputStream;
import java.nio.ByteBuffer;

public record StreamChunk(
        StreamRegistry registry,
//        PodIdRestart pod,
//        StreamType stream,
//        int rollingSequenceID,
        long startPos,
        long length,
        ByteBuffer chunk) implements Comparable<StreamChunk> {

    public String getPk() {
        return String.format("%s-%s-%d-%d",
                registry.podRestart().podId(), registry.stream().getName(), registry.rollingSequenceId(), startPos);
    }

    public InputStream getInputStream() {
        return new ByteBufferInputStream(chunk);
    }

    @Override
    public int compareTo(StreamChunk o) {
        return (int) (startPos - o.startPos);
    }
}
