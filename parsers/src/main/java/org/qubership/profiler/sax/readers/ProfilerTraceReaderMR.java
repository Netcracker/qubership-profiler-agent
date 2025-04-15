package org.qubership.profiler.sax.readers;

import org.qubership.profiler.io.CallRowid;
import org.qubership.profiler.io.exceptions.ErrorSupervisor;
import org.qubership.profiler.sax.raw.MultiRepositoryVisitor;
import org.qubership.profiler.sax.raw.RepositoryVisitor;
import org.qubership.profiler.sax.raw.TreeRowid;
import org.qubership.profiler.timeout.ProfilerTimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Scope("prototype")
public class ProfilerTraceReaderMR {
    private final MultiRepositoryVisitor mrv;

    @Autowired
    ProfilerTraceReaderFactory traceReaderFactory;

    private ProfilerTraceReaderMR() {
        throw new RuntimeException("no-args constructor not supported");
    }

    public ProfilerTraceReaderMR(MultiRepositoryVisitor mrv) {
        this.mrv = mrv;
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
