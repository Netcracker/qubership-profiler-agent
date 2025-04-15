package org.qubership.profiler.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface CallReaderFactory {
    List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer) throws IOException;

    List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer, Set<String> nodes) throws IOException;

    List<ICallReader> collectCallReaders(Map<String, String[]> params, TemporalRequestParams temporal, CallListener listener, CallFilterer filterer, Set<String> nodes, boolean readDictionary) throws IOException;
}
