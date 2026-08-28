package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.io.CallRowid;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.raw.MultiRepositoryVisitor;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.raw.TreeRowid;
import com.netcracker.profiler.timeout.ProfilerTimeoutException;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Prototype-scoped class - create instances via {@code ProfilerTraceReaderMRFactory}.
 */
public class ProfilerTraceReaderMR {
    private final MultiRepositoryVisitor mrv;
    private final ProfilerTraceReaderFactory traceReaderFactory;

    @AssistedInject
    public ProfilerTraceReaderMR(
            @Assisted("mrv") MultiRepositoryVisitor mrv,
            ProfilerTraceReaderFactory traceReaderFactory) {
        this.mrv = mrv;
        this.traceReaderFactory = traceReaderFactory;
    }

    public void read(CallRowid[] callIds) {
        read(callIds, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public void read(CallRowid[] callIds, long begin, long end) {
        if (callIds.length == 0)
            return;

        Map<String, List<TreeRowid>> x = new HashMap<String, List<TreeRowid>>();
        for (CallRowid callId : callIds) {
            List<TreeRowid> treeRowids = x.get(callId.file);
            if (treeRowids == null)
                x.put(callId.file, treeRowids = new ArrayList<TreeRowid>());
            treeRowids.add(callId.rowid);
        }

        for (Map.Entry<String, List<TreeRowid>> entry : x.entrySet()) {
//            File dumpRoot = new File(root, entry.getKey());

            RepositoryVisitor rv = mrv.visitRepository(entry.getKey());
            try {
                ProfilerTraceReader reader = traceReaderFactory.newTraceReader(rv, entry.getKey());
                reader.read(entry.getValue(), begin, end);
            } catch (Error|ProfilerTimeoutException e) {
                throw e;
            } catch (Throwable t) {
                ErrorSupervisor.getInstance().error("Unable to read " + entry.getKey(), t);
            }
            rv.visitEnd();
        }

        mrv.visitEnd();
    }
}
