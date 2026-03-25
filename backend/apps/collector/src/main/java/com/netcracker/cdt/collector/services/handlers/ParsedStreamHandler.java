package com.netcracker.cdt.collector.services.handlers;

import com.netcracker.cdt.collector.parsers.StreamParser;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.Sizeable;
import com.netcracker.common.models.pod.streams.StreamRegistry;

import java.io.IOException;

public final class ParsedStreamHandler<T extends Sizeable> extends StreamHandler {
    final StreamParser<T> streamParser;
    boolean resetRequired;

    public ParsedStreamHandler(PersistenceService persistence,
                               StreamParser<T> parser,
                               StreamDumper streamDumper,
                               StreamRegistry streamRegistry,
                               boolean resetRequired) {
        super(persistence, streamDumper, streamRegistry);
        this.streamParser = parser;
        this.resetRequired = resetRequired;
    }

    public void receive(byte[] b, int off, int len) {
        streamParser.receiveData(b, off, len, null);
        streamParser.saveData(persistence);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // TODO rewrite StreamHandler (extends OutputStream)
    }

    public boolean flushCompressorIfNeeded() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }

}
