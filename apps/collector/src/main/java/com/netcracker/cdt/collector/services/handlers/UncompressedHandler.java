package com.netcracker.cdt.collector.services.handlers;

import com.netcracker.cdt.collector.common.transport.ProfilerProtocolException;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.persistence.PersistenceService;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static com.netcracker.common.ProtocolConst.MAX_FLUSH_INTERVAL_MILLIS;

// for uncompressed streams -- like heap dump, which already compressed at the agent
public final class UncompressedHandler<T extends Sizeable> extends StreamHandler {
    final OutputStream out;
    final UUID streamHandle;
    long lastOffset = 0L;
    long lastFlushed = -1;

    public UncompressedHandler(PersistenceService persistence,
                               StreamDumper streamDumper,
                               StreamRegistry streamRegistry,
                               UUID streamHandle,
                               int compressorBufferSize) {
        super(persistence, streamDumper, streamRegistry);
        this.streamHandle = streamHandle;

        try {
            this.out = new BufferedOutputStream(this, compressorBufferSize);
        } catch (Exception e) {
            throw new ProfilerProtocolException(e);
        }
    }

    public void receive(byte[] b, int off, int len) {
        try {
            out.write(b, off, len);

            if (System.currentTimeMillis() - lastFlushed > MAX_FLUSH_INTERVAL_MILLIS ) {
                out.flush();
            }
        } catch (IOException e) {
            throw new ProfilerProtocolException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        streamFacade.saveStreamChunk(streamHandle, streamRegistry, lastOffset, b, off, len);
        lastFlushed = System.currentTimeMillis();
        lastOffset += len;
    }

    public boolean flushCompressorIfNeeded() {
        if (System.currentTimeMillis() - lastFlushed < MAX_FLUSH_INTERVAL_MILLIS) {
            return false;
        }
        try {
            out.flush();
            return true;
        } catch (IOException e) {
            throw new ProfilerProtocolException(e);
        }
    }

    public void close() throws IOException {
        if (out != null) {
            out.close();
        }
    }
}
