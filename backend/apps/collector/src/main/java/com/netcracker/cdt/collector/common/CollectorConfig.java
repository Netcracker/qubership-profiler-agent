package com.netcracker.cdt.collector.common;

import com.netcracker.common.models.StreamType;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@LookupIfProperty(name = "service.type", stringValue = "collector")
@ApplicationScoped
public class CollectorConfig {
    @ConfigProperty(name = "pod.collector.retention.period", defaultValue="1209600000") // LOG_RETENTION_PERIOD
    long logRetentionPeriod; // Default to 2 weeks

    @ConfigProperty(name = "pod.collector.max.size.kb", defaultValue="204800") // LOG_MAX_SIZE_KB
    long logMaxSizeKB; // Default to 200 MB per pod


    @ConfigProperty(name = "pod.collector.stream.chunk.size", defaultValue="3072") // STREAM_CHUNK_SIZE
    int streamChunkSize; // Default to 3k

    @ConfigProperty(name = "pod.collector.stream.rotation.period", defaultValue = "300000") // STREAM_ROTATION_PERIOD
    long streamRotationPeriod; // Default is 5m. Specified in milliseconds for those streams that require rotation

    @ConfigProperty(name = "pod.collector.num.heavy.clients", defaultValue="100") // NUM_HEAVY_CLIENTS
    int numHeavyClients;

    @ConfigProperty(name = "pod.collector.num.idle.clients", defaultValue="1000") // NUM_IDLE_CLIENTS
    int numIdleClients;

    @ConfigProperty(name = "pod.collector.stat.persist")
    String podStatCron;


    public int getMaxConnections() {
        return (numHeavyClients + numIdleClients) * 2;
    }


    public int getMaxOpenStreams() {
        return (numHeavyClients + numIdleClients) * StreamType.values().length;
    }

    public int maxOpenStreams() {
        return (numHeavyClients + numIdleClients) * StreamType.withRotation().size();
    }

    public String getPodStatCron() {
        return podStatCron;
    }

    public int getCompressorBufferSize() {
        //size of query to insert into stream_chunks is approx 350 bytes. need to post significantly more than that
        //size of disk pages in cassandra is typically 4 KB
        //data is already compressed. cassandra won't be able to compress it much further
        return streamChunkSize;
    }


    public Long getLogRetentionPeriod() {
        return logRetentionPeriod;
    }

    public long getLogRetentionPeriod(Long logRetentionPeriod) {
        return logRetentionPeriod == null ? getLogRetentionPeriod() : logRetentionPeriod;
    }

    public long getLogMaxSize() {
        return logMaxSizeKB * 1024L;
    }

    public long getLogMaxSize(Long logMaxSize) {
        return logMaxSize == null ? getLogMaxSize() : logMaxSize;
    }

    public long getRotationPeriod(StreamType streamType) {
        if (!streamType.isRotationRequired()) {
            return 0;
        }

        if (streamRotationPeriod > 0) { // If stream rotation period is specified explicitly
            return streamRotationPeriod;
        }

        // Otherwise rotate at least every hour. But at least 3 times during retention period
        return Math.min(logRetentionPeriod / 3, 3_600_000L);
    }

    public long getRequiredRotationSize(StreamType streamType) {
        if (!streamType.isRotationRequired()) {
            return 0;
        }

        // Otherwise rotate at least every 2 MB (2097152 bytes). But at least 10 times during retention period
        return Math.min(getLogMaxSize() / 10L, 2_097_152L);
    }

}
