package com.netcracker.cdt.collector.common.models;

import com.netcracker.common.models.StreamType;

import java.util.Collection;
import java.util.UUID;

public record StreamRotatedInfo(StreamType stream,
                                int rollingSequenceId,
                                UUID newStreamId,
                                Collection<UUID> cleanedUpStreamIDs,
                                long uploadedSize) {

    public StreamRotatedInfo uploaded(long uploadedBytes) {
        return new StreamRotatedInfo(stream, rollingSequenceId, newStreamId, cleanedUpStreamIDs, uploadedBytes);
    }

    public static StreamRotatedInfo of(StreamType stream, int sequenceId, UUID uuid, Collection<UUID> cleanedUpStreams) {
        return new StreamRotatedInfo(stream, sequenceId, uuid, cleanedUpStreams, 0);
    }
}
