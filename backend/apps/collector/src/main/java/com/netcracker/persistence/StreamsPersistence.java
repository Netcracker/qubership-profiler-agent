package com.netcracker.persistence;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.streams.StreamChunk;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.persistence.op.Operation;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface StreamsPersistence {
    // persist
    Integer getRollingSequenceId(PodIdRestart pod, StreamType stream);

    Optional<StreamRegistry> getStreamRegistryById(PodIdRestart pod, StreamType stream, Integer rollingSequenceId);

    Operation upsertStreamRegistry(StreamRegistry sr);

    Operation insertStreamChunk(StreamChunk chunk);

    // search
    List<StreamRegistry> getRegistries(List<PodIdRestart> pod, StreamType streamType, TimeRange range);

    List<StreamRegistry> getRegistries(PodIdRestart pod, StreamType streamType, TimeRange range);

    List<StreamRegistry> getLatestRegistry(PodIdRestart pod, StreamType streamType);

    InputStream getStream(StreamRegistry sr);

    // cleanup
    List<StreamRegistry> getStreamRegistries(PodIdRestart pod, StreamType streamType, TimeRange range, int limit);

    List<StreamRegistry> getStreamRegistries(PodIdRestart pod, Instant timeTo, int limit);

    List<StreamRegistry> escStreamRegistries(PodIdRestart pod, TimeRange range, int limit);

    Operation deleteStreamData(StreamRegistry sr); // delete chunks for stream

    Operation deleteStreamRegistry(StreamRegistry sr); // delete registry

}
