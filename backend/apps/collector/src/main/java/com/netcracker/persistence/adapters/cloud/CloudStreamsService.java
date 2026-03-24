package com.netcracker.persistence.adapters.cloud;

import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.streams.StreamChunk;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.persistence.StreamsPersistence;
import com.netcracker.persistence.op.Operation;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@LookupIfProperty(name = "service.persistence", stringValue = PersistenceType.CLOUD)
@ApplicationScoped
public class CloudStreamsService implements StreamsPersistence {

    @Override
    public Integer getRollingSequenceId(PodIdRestart pod, StreamType stream) {
        return null;
    }

    @Override
    public Optional<StreamRegistry> getStreamRegistryById(PodIdRestart pod, StreamType stream, Integer rollingSequenceId) {
        return Optional.empty();
    }

    @Override
    public Operation upsertStreamRegistry(StreamRegistry sr) {
        return Operation.empty();
    }

    @Override
    public Operation insertStreamChunk(StreamChunk chunk) {
        return Operation.empty();
    }

    @Override
    public List<StreamRegistry> getStreamRegistries(PodIdRestart pod, StreamType streamType, TimeRange range, int limit) {
        return List.of();
    }

    @Override
    public List<StreamRegistry> getStreamRegistries(PodIdRestart pod, Instant timeTo, int limit) {
        return List.of();
    }

    @Override
    public List<StreamRegistry> escStreamRegistries(PodIdRestart pod, TimeRange range, int limit) {
        return List.of();
    }

    @Override
    public Operation deleteStreamData(StreamRegistry sr) {
        return Operation.empty();
    }

    @Override
    public Operation deleteStreamRegistry(StreamRegistry sr) {
        return Operation.empty();
    }

    @Override
    public List<StreamRegistry> getRegistries(List<PodIdRestart> pod, StreamType streamType, TimeRange range) {
        return List.of();
    }

    @Override
    public List<StreamRegistry> getRegistries(PodIdRestart pod, StreamType streamType, TimeRange range) {
        return List.of();
    }

    @Override
    public List<StreamRegistry> getLatestRegistry(PodIdRestart pod, StreamType streamType) {
        return List.of();
    }

    @Override
    public InputStream getStream(StreamRegistry sr) {
        return null;
    }
}
