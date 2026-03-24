package com.netcracker.cdt.collector.services;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netcracker.cdt.collector.common.StreamNotInitializedException;
import com.netcracker.cdt.collector.common.models.StreamInfoRequest;
import com.netcracker.cdt.collector.parsers.SuspendStreamParser;
import com.netcracker.cdt.collector.common.CollectorConfig;
import com.netcracker.cdt.collector.common.models.StreamRotatedInfo;
import com.netcracker.cdt.collector.parsers.DictionaryStreamParser;
import com.netcracker.cdt.collector.parsers.ParamsStreamParser;
import com.netcracker.cdt.collector.services.handlers.CompressorHandler;
import com.netcracker.cdt.collector.services.handlers.ParsedStreamHandler;
import com.netcracker.cdt.collector.services.handlers.StreamHandler;
import com.netcracker.cdt.collector.services.handlers.UncompressedHandler;
import com.netcracker.common.Time;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.streams.StreamChunk;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.PersistenceService;
import io.quarkus.arc.Lock;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

@LookupIfProperty(name = "service.type", stringValue = "collector")
@ApplicationScoped
public class StreamDumper {
    @Inject
    Time time;
    @Inject
    CollectorConfig config;

    @Inject
    PersistenceService persistence;

    @Inject
    PodDumper podDumper;

    private static Cache<UUID, StreamHandler> openStreams; // TODO: check: each open stream retains 300 KB of heap

    @PostConstruct
    public void init() {
        openStreams = CacheBuilder.newBuilder().
                maximumSize(config.getMaxOpenStreams()).build();
    }

    public long getRotationPeriod(StreamType stream) {
        return config.getRotationPeriod(stream);
    }

    public long getRequiredRotationSize(StreamType stream) {
        return config.getRequiredRotationSize(stream);
    }

    public int getRollingSequenceId(UUID streamID) {
        var sr = getStreamHandler(streamID).registry();
        return sr.rollingSequenceId();
    }

    public StreamHandler getStreamHandler(UUID streamHandle) {
        if (streamHandle == null) {
            throw new StreamNotInitializedException(streamHandle);
        }
        if (openStreams == null) {
            throw new StreamNotInitializedException(streamHandle);
        }
        StreamHandler handler = openStreams.getIfPresent(streamHandle);
        if (handler == null) {
            throw new StreamNotInitializedException(streamHandle);
        }
        return handler;
    }

    @DB
    public StreamRotatedInfo streamOpened(StreamInfoRequest streamRequest) {
        // If such request is received, the previous streams may be removed from the in-memory map
        Collection<UUID> cleanedUpStreams = cleanupPreviousStreams(streamRequest.podId(), streamRequest.stream());

        UUID uuid = UUID.randomUUID();
        int seqId = streamRequest.forceRequestedRollingSequenceId() ?
                streamRequest.requestedRollingSequenceId() :
                calculateRollingSequenceId(streamRequest);

        var registry = StreamRegistry.create(streamRequest, seqId);
        storeHandle(uuid, registry, streamRequest.resetRequired());

        persistence.batch.execute(
                persistence.streams.upsertStreamRegistry(registry)
        );
        int sequenceId = getRollingSequenceId(uuid); // TODO check: should == seqId
        return StreamRotatedInfo.of(streamRequest.stream(), sequenceId, uuid, cleanedUpStreams);
    }

    @DB
    Collection<UUID> cleanupPreviousStreams(String podId, StreamType stream) {
        Log.tracef("[%s] Check for old streams of %s from openStreams", podId, stream);
        // Protect against ConcurrentModificationException
        List<UUID> toRemove = new ArrayList<>();
        for (Map.Entry<UUID, StreamHandler> entry : openStreams.asMap().entrySet()) {
            StreamHandler handler = entry.getValue();
            var sr = handler.registry();
            if (!(StringUtils.equals(sr.podRestart().podId(), podId) && sr.stream().equals(stream))) {
                continue;
            }
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Cleaning up openStreams map %s|%d with uuid=%s",
                        sr.podRestart().podId(),
                        sr.stream().getName(), sr.rollingSequenceId(),
                        entry.getKey().toString());
            }
            toRemove.add(entry.getKey());
            closeStreamHandler(handler);
        }
        if (!toRemove.isEmpty()) {
            Log.debugf("[%s] Removing %d old streams of %s from openStreams", podId, toRemove.size(), stream);
            removeHandles(toRemove);
        }
        return toRemove;
    }

    @DB
    public Collection<UUID> cleanupPodStreams(String podId) { // pod restart/reconnect
        Log.tracef("Check for streams of %s from openStreams map", podId);
        // Protect against ConcurrentModificationException
        List<UUID> toRemove = new ArrayList<>();
        for (Map.Entry<UUID, StreamHandler> entry : openStreams.asMap().entrySet()) {
            StreamHandler handler = entry.getValue();
            var sr = handler.registry();
            if (!StringUtils.equals(sr.podRestart().podId(), podId)) {
                continue;
            }
            if (Log.isTraceEnabled()) {
                Log.tracef("Cleaning up openStreams map %s:%s|%d with uuid=%s",
                        sr.podRestart().podId(), sr.stream().getName(), sr.rollingSequenceId(),
                        entry.getKey().toString());
            }
            toRemove.add(entry.getKey());
            closeStreamHandler(handler);
        }
        if (!toRemove.isEmpty()) {
            Log.debugf("Removing %d old streams of %s from openStreams map", toRemove.size(), podId);
            removeHandles(toRemove);
        }
        return toRemove;

    }

    @Lock // cache modification
    void storeHandle(UUID streamHandle, StreamRegistry sr, boolean resetRequired) {
        var handler = switch (sr.stream()) {
            case SUSPEND ->
                    new ParsedStreamHandler<>(persistence, SuspendStreamParser.create(sr.podRestart()), this, sr, resetRequired);
            case PARAMS ->
                    new ParsedStreamHandler<>(persistence, ParamsStreamParser.create(sr.podRestart()), this, sr, resetRequired);
            case DICTIONARY ->
                    new ParsedStreamHandler<>(persistence, DictionaryStreamParser.create(sr.podRestart(), -1), this, sr, resetRequired);
            case HEAP ->
                    new UncompressedHandler(persistence, this, sr, streamHandle, config.getCompressorBufferSize());
            default -> new CompressorHandler(persistence, this, sr, streamHandle, config.getCompressorBufferSize());
        };
//        StreamHandler<?> bean = context.getBean(StreamHandler.class, this, sr, resetRequired);
        openStreams.put(streamHandle, handler);
        if (Log.isDebugEnabled()) {
            Log.debugf("[%s] storeHandle %s - %s", sr.podRestart().oldPodName(), streamHandle, sr.stream());
        }
    }

    @Lock // cache modification
    public void removeHandles(Collection<UUID> toRemove) {
        for (UUID uuid : toRemove) {
            openStreams.invalidate(uuid);
            Log.tracef("UUID %s removed during rotation", uuid.toString());
        }
    }

    @DB
    public int calculateRollingSequenceId(StreamInfoRequest req) {
        Integer rollingSequenceId = persistence.streams.getRollingSequenceId(req.id(), req.stream());
        if (rollingSequenceId == null) {
            rollingSequenceId = -1;
        }
        // It is allowed to request larger sequences since ids we need to synchronize calls and reactorCalls
        return Math.max(req.requestedRollingSequenceId(), rollingSequenceId + 1);
    }


    @DB
    public void flushStreams(Set<UUID> streamIDs) {
        for (Iterator<UUID> it = streamIDs.iterator(); it.hasNext(); ) {
            UUID uuid = it.next();
            StreamHandler handler = openStreams.getIfPresent(uuid);
            if (handler == null) {
                Log.debugf("Not flushing stream {} as it probably has been cleaned up", uuid);
                it.remove();
                continue;
            }
            if (handler.flushCompressorIfNeeded()) {
                Log.tracef("Flushing stream {}. Removing it from flush candidates", uuid);
                it.remove();
            }
        }
    }

    @DB
    public void closeAndForget(UUID streamHandle) {
        // When dump is imported, map of active stream handlers may get overwhelmed
        StreamHandler handler = openStreams.getIfPresent(streamHandle);
        if (handler == null) {
            return;
        }
        var sr = handler.registry();
        if (Log.isDebugEnabled()) {
            Log.debugf("closeAndForget %s with uuid=%s", sr.screenName(), streamHandle);
        }
        closeStreamHandler(handler);
        persistence.batch.execute(
                persistence.streams.upsertStreamRegistry(sr.close(time.now())) // mark as finished, update size
        );
        openStreams.invalidate(streamHandle);
    }

    @DB
    protected void closeStreamHandler(StreamHandler handler) {
        try {
            handler.close();
        } catch (IOException e) {
            //do not fail stream rotation because of this
            var sr = handler.registry();
            Log.errorf(e, "ProfilerProtocolException: Failed to close previous compressor %s", sr.screenName());
        }
    }

    /**
     * Dictionary and params streams are saved with infinite TTL
     * other streams - with limited to lifetime
     *
     * @param streamID some id
     * @param data     some data
     * @param offset   offset of data in the data buffer
     * @param length   length of data in the data buffer
     */
    @DB
    public void receiveData(UUID streamID, byte[] data, int offset, int length) {
        StreamHandler handler = getStreamHandler(streamID);
        StreamRegistry sr = handler.registry();
        if (Log.isTraceEnabled()) {
            Log.tracef("received %d bytes of data for stream %s", length, sr.stream());
        }
        handler.receive(data, offset, length);
        podDumper.received(sr, length);
    }

    /**
     * Dictionary and params streams are saved with infinite TTL
     * other streams - with limited to lifetime
     *
     * @param registry some registry
     * @param data     some data
     * @param offset   offset of data in the data buffer
     * @param length   length of data in the data buffer
     */
    @DB
    public void saveStreamChunk(UUID streamHandle, StreamRegistry registry, long startPos, byte[] data, int offset, int length) {

        if (Log.isTraceEnabled()) {
            Log.tracef("Save stream chunk %s: len %d", registry.screenName(), registry, (long) length);
        }

        long start = Log.isTraceEnabled() ? System.currentTimeMillis() : -1;

        ByteBuffer chunkBuf = ByteBuffer.wrap(data, offset, length);

        podDumper.persisted(registry, length);

        StreamChunk chunk = new StreamChunk(registry, startPos, length, chunkBuf);
//        var accLength = registry.dataSize().val(false); // gzipped
//        StreamChunk chunk = new StreamChunk(registry, offset, accLength, chunkBuf);
//        StreamChunk chunk = new StreamChunk(registry, offset, length, chunkBuf);

        persistence.batch.execute(
                persistence.streams.insertStreamChunk(chunk)
        );

        if (Log.isTraceEnabled()) {
            long spent = System.currentTimeMillis() - start;
            Log.tracef("Saved stream chunk of %d bytes. Spent %d ms", length, spent);
        }
    }

}
