package com.netcracker.common.models.pod.stat;

import com.netcracker.common.models.pod.PodInfo;

import java.time.Instant;

public record HeapDump(PodInfo pod, String seqId, Instant createdAt, long compressed, long original) implements Comparable<HeapDump> {

    @Override
    public int compareTo(HeapDump o) {
        return -createdAt.compareTo(o.createdAt);
    }
}
