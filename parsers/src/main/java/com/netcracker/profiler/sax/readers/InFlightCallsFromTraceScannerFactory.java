package com.netcracker.profiler.sax.readers;

import com.google.inject.assistedinject.Assisted;

public interface InFlightCallsFromTraceScannerFactory {
    InFlightCallsFromTraceScanner create(
            @Assisted("rootReference") String rootReference,
            @Assisted("startFileIndex") int startFileIndex
    );
}
