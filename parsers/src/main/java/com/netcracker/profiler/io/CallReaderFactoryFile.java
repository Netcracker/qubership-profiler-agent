package com.netcracker.profiler.io;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;


@Component
@Profile("filestorage")
public class CallReaderFactoryFile implements CallReaderFactory {

    @Autowired
    ApplicationContext context;

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

            cr = context.getBean(CallReaderFile.class, listener, filterer, nodes, readDictionary, dumpDirs);
        long clientServerTimeDiff = (long) (Math.abs(temporal.clientUTC - temporal.serverUTC) * 1.5);
        cr.setTimeFilterCondition(temporal.timerangeFrom - clientServerTimeDiff, temporal.timerangeTo + clientServerTimeDiff);
        return Collections.singletonList(cr);
    }
}
