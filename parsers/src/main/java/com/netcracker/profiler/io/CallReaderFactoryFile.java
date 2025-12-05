package com.netcracker.profiler.io;

import java.io.IOException;
import java.util.*;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class CallReaderFactoryFile implements CallReaderFactory {

    private final CallReaderFileFactory callReaderFileFactory;

    @Inject
    public CallReaderFactoryFile(CallReaderFileFactory callReaderFileFactory) {
        this.callReaderFileFactory = callReaderFileFactory;
    }

    public List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer) throws IOException {
        return collectCallReaders(params, temporal, listener, filterer, null);
    }

    public List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer, Set<String> nodes) throws IOException {
        return collectCallReaders(params, temporal, listener, filterer, nodes, true);
    }

    public List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer, Set<String> nodes, boolean readDictionary) throws IOException {
        ICallReader cr;
        Set<String> dumpDirs = null;
        if(params != null) {
            String[] dumpDirsArr = params.get("dumpDirs");
            dumpDirs = dumpDirsArr == null ? null : new HashSet<String>(Arrays.asList(dumpDirsArr));
        }

        cr = callReaderFileFactory.create(listener, filterer, nodes, readDictionary, dumpDirs);
        cr.setTimeFilterCondition(temporal.timerangeFrom, temporal.timerangeTo);
        return Collections.singletonList(cr);
    }
}
