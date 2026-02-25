package com.netcracker.profiler.io.call;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

/**
 * Factory for creating CallDataReader instances based on file format version.
 * Uses Guice MapBinder to manage the mapping of format versions to implementations.
 */
@Singleton
public class CallDataReaderFactory {
    private final Map<Integer, Provider<CallDataReader>> readers;
    private final Provider<CallDataReader> defaultReader;

    @Inject
    public CallDataReaderFactory(Map<Integer, Provider<CallDataReader>> readers, Provider<CallDataReader_00> defaultReader) {
        this.readers = readers;
        // Guice can't inject Map<Integer, Provider<? extends CallDataReader>>, so we have to do an unchecked cast here
        @SuppressWarnings({"rawtypes", "unchecked"})
        Provider<CallDataReader> reader = (Provider<CallDataReader>) (Provider) defaultReader;
        // Default reader is format 0
        this.defaultReader = readers.getOrDefault(0, reader);
    }

    /**
     * Creates a CallDataReader for the specified file format.
     *
     * @param fileFormat The file format version (0-4)
     * @return CallDataReader instance for the specified format
     */
    public CallDataReader createReader(int fileFormat) {
        return readers.getOrDefault(fileFormat, defaultReader).get();
    }
}
