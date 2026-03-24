package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.cdt.ui.services.tree.CallTreeRequest;
import com.netcracker.cdt.ui.services.tree.data.ProfiledTree;
import com.netcracker.profiler.model.CallRowId;
import com.netcracker.profiler.model.TreeRowId;
import com.netcracker.profiler.timeout.ProfilerTimeoutException;
import io.quarkus.logging.Log;

import java.util.*;

public class TraceRequestReader {
    protected final TreeDataLoader treeDataLoader;
    protected final CallTreeRequest request;

    public TraceRequestReader(TreeDataLoader storage, CallTreeRequest request) {
        this.treeDataLoader = storage;
        this.request = request;
    }

    public ProfiledTree read() {
        var arr = new CallRowId[request.callIds().size()];
        return read(request.callIds().toArray(arr), request.begin(), request.end());
    }

    ProfiledTree read(CallRowId[] callIds, long begin, long end) {
        if (callIds.length == 0)
            return null;

        ProfiledTree tree = null;
        var files = groupByFile(callIds);
        for (var entry : files.entrySet()) {
            try {
                var reader = new TracePodReader(treeDataLoader, request, entry.getKey());
                var t = reader.readTraces(entry.getValue(), begin, end);
                if (tree == null) {
                    tree = t;
                } else {
                    tree.merge(t);
                }
            } catch (Error | ProfilerTimeoutException e) {
                throw e;
            } catch (Throwable t) {
                Log.errorf(t, "Unable to read " + entry.getKey());
            }
        }
        return tree;
    }

    Map<String, List<TreeRowId>> groupByFile(CallRowId[] callIds) {
        var x = new HashMap<String, List<TreeRowId>>();
        for (CallRowId callId : callIds) {
            List<TreeRowId> treeRowIds = x.get(callId.file());
            if (treeRowIds == null)
                x.put(callId.file(), treeRowIds = new ArrayList<>());
            treeRowIds.add(callId.treeRow());
        }
        return x;
    }
}
