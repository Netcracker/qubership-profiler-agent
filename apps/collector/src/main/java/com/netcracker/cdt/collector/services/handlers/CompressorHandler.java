package com.netcracker.cdt.collector.services.handlers;

import com.netcracker.cdt.collector.common.transport.ProfilerProtocolException;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.PersistenceType;
import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.persistence.PersistenceService;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static com.netcracker.common.ProtocolConst.MAX_FLUSH_INTERVAL_MILLIS;

public final class CompressorHandler<T extends Sizeable> extends StreamHandler {
    final OutputStream compressor;
    final UUID streamHandle;
    long lastOffset = 0L;
    long lastFlushed = -1;
    CollectorCallsExtractor collectorCallsExtractor = null;

    public CompressorHandler(PersistenceService persistence,
                             StreamDumper streamDumper,
                             StreamRegistry streamRegistry,
                             UUID streamHandle,
                             int compressorBufferSize) {
        super(persistence, streamDumper, streamRegistry);
        this.streamHandle = streamHandle;

        try {
//            this.compressor = new GZIPOutputStream(new BufferedOutputStream(this, compressorBufferSize), false);
//            this.compressor = new GZIPOutputStream(new BufferedOutputStream(this, compressorBufferSize), true);
//            LZ4Factory.fastestJavaInstance().fastCompressor().
//            this.compressor = new LZ4BlockOutputStream(this, compressorBufferSize);
            this.compressor = new BufferedOutputStream(this, compressorBufferSize);
            if (persistence.getType().equals(PersistenceType.CLOUD) && streamRegistry.stream().equals(StreamType.CALLS)) {
                this.collectorCallsExtractor = new CollectorCallsExtractor(persistence, streamRegistry);
            }
//        } catch (IOException e) {
        } catch (Exception e) {
            throw new ProfilerProtocolException(e);
        }
    }

    public void receive(byte[] b, int off, int len) {
        try {
            compressor.write(b, off, len);
            if (System.currentTimeMillis() - lastFlushed > MAX_FLUSH_INTERVAL_MILLIS) {
                compressor.flush();
            }
        } catch (IOException e) {
            throw new ProfilerProtocolException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {

        if (collectorCallsExtractor != null && streamRegistry.stream().equals(StreamType.CALLS)) {

            // TODO: Is it really necessary?
            // You may need to recreate the extractor if it completes its work before it processes all calls
//            if (collectorCallsExtractor.isFinished()) {
//                this.collectorCallsExtractor = new CollectorCallsExtractor(persistence, streamRegistry);
//            }

            collectorCallsExtractor.add(b, len);
        } else {
            streamFacade.saveStreamChunk(streamHandle, streamRegistry, lastOffset, b, off, len);
        }

        lastFlushed = System.currentTimeMillis();
        lastOffset += len;
    }

    public boolean flushCompressorIfNeeded() {
        if (System.currentTimeMillis() - lastFlushed < MAX_FLUSH_INTERVAL_MILLIS) {
            return false;
        }
        try {
            compressor.flush();
            return true;
        } catch (IOException e) {
            throw new ProfilerProtocolException(e);
        }
    }

    public void close() throws IOException {
        if (compressor != null) {
//            compressor.flush();
            compressor.close();
        }
    }
}
