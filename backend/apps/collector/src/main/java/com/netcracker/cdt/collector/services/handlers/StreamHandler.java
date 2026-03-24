package com.netcracker.cdt.collector.services.handlers;

import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.pod.streams.StreamRegistry;

import java.io.IOException;
import java.io.OutputStream;

public abstract sealed class StreamHandler extends OutputStream
        permits ParsedStreamHandler, CompressorHandler, UncompressedHandler {
    protected final PersistenceService persistence;
    protected final StreamDumper streamFacade;
    protected final StreamRegistry streamRegistry;

    public StreamHandler(PersistenceService persistence,
                         StreamDumper streamDumper,
                         StreamRegistry streamRegistry) {
        if (streamDumper == null) {
            throw new RuntimeException("streamDumper can't be null");
        }
        this.streamRegistry = streamRegistry;
        this.streamFacade = streamDumper;
        this.persistence = persistence;
    }

    public StreamRegistry registry() {
        return streamRegistry;
    }

    public abstract boolean flushCompressorIfNeeded();

    public abstract void receive(byte[] b, int off, int len);

    public abstract void write(byte[] b, int off, int len) throws IOException ; // as OutputStream

    public abstract void close() throws IOException ; // as OutputStream

    @Override
    public void write(int b) throws IOException { // as OutputStream
        throw new RuntimeException("Should never be used");
    }

}
