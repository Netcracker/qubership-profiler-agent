package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.cdt.ui.services.tree.data.ProfiledTree;
import com.netcracker.cdt.ui.services.tree.CallTreeRequest;
import com.netcracker.common.models.meta.ClobIndex;
import com.netcracker.common.models.meta.DictionaryIndex;
import com.netcracker.common.models.Pair;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.SuspendRange;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.ParamTypes;
import com.netcracker.profiler.model.TreeRowId;
import com.netcracker.common.models.meta.Value;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.util.*;

public class TracePodReader {
    protected final TreeDataLoader treeDataLoader;
    protected final String podReference;

    final DictionaryIndex dictIdx;
    final ClobIndex clobIdx;
    final SuspendRange sRange;

    public TracePodReader(TreeDataLoader storage, CallTreeRequest request, String rootReference) {
        this(storage, rootReference, request.paramsTrimSize());
    }

    TracePodReader(String rootReference) {
        this(null, rootReference, 100);
    }

    private TracePodReader(TreeDataLoader storage, String rootReference, int trimSize) {
        this.treeDataLoader = storage;
        this.podReference = rootReference;

        dictIdx = new DictionaryIndex();
        clobIdx = new ClobIndex(trimSize);
        sRange = new SuspendRange();
    }

    public ProfiledTree readTraces(List<TreeRowId> treeRowIds, long begin, long end) {
        if (treeRowIds.isEmpty()) return null;
        // sorted by [file, bufferOffset, recordIndex]
        Collections.sort(treeRowIds);

        var idx = new TreeSet<Integer>();
        for (var tid : treeRowIds) {
            idx.add(tid.traceFileIndex);
        }
        if (idx.isEmpty()) return null;

        treeDataLoader.loadSuspendLog(sRange, podReference, begin, end);

        ProfiledTree tree = null;
        for (var tid : idx) {
            try (var traceStream = treeDataLoader.openTraceStream(podReference, tid)) {
                var filtered = treeRowIds.stream().filter(t -> (t.traceFileIndex == tid)).toList();
                var t = readTrace(traceStream, tid, filtered);
                if (tree == null) {
                    tree = t;
                } else {
                    tree.merge(t);
                }
            } catch (IOException e) {
                Log.errorf(e, "could not open trace stream %d for %s", tid, podReference);
            }
        }
        return tree;
    }

    public ProfiledTree readTrace(DataInputStreamEx traceStream, int fileIdx, List<TreeRowId> treeRowIds) {
        try {
            var res = parseTraces(traceStream, treeRowIds);
            var ids = res.key();
            var threads = res.value();

            treeDataLoader.loadMeta(dictIdx, podReference, ids);
            treeDataLoader.loadClobs(clobIdx);

            ProfiledTree tree = null;
            for (var tb : threads.values()) {
                var t = tb.getTree();
                if (tree == null) {
                    tree = t;
                } else {
                    t.merge(tree);
                }
            }
            Log.tracef("[%s:%d] Load %d rows from", podReference, fileIdx, treeRowIds.size());
            return tree;
        } catch (Exception t) {
            Log.errorf(t, "[%s:%d] Error while reading profiling tree for %d rows", podReference, fileIdx, treeRowIds.size());
            return null;
        }
    }

    Pair<BitSet, HashMap<Long, TreeTraceBuilder>> parseTraces(DataInputStreamEx traceStream, List<TreeRowId> treeRowIds) throws IOException {
        final var threads = new HashMap<Long, TreeTraceBuilder>();

        BitSet ids = new BitSet();
        try {
            long timerStartTime = traceStream.readLong();
            for (var treeRowid : treeRowIds) {
                int tracePos = traceStream.position();
                if (tracePos < treeRowid.bufferOffset) {
                    if (!Log.isTraceEnabled()) {
                        traceStream.skipBytes(treeRowid.bufferOffset - tracePos);
                    } else {
                        var c = treeRowid.bufferOffset - tracePos;
                        var s = new StringBuffer();
                        for (int i = 0; i < c; i++) {
                            var pos = traceStream.position();
                            var b = traceStream.read();
                            if (c - pos < 100) {
                                if (pos % 16 == 0) {
                                    s.append(String.format("\n%5d | %03X =>  ", pos, pos));
                                } else if (pos % 8 == 0) {
                                    s.append(" | ");
                                }
                                s.append(String.format(" [%4d x%02X]", b, (byte) b));
                            }
                        }
                        Log.tracef("Was %d, skipped %d => %d | %02X \nData:\n %s", tracePos, c, traceStream.position(), traceStream.position(), s.toString());
                    }
                }

                Long currentThreadId = traceStream.readLong();
                threads.putIfAbsent(currentThreadId, TreeTraceBuilder.create(dictIdx, sRange, clobIdx, treeRowid));
                var ttv = threads.get(currentThreadId);
                parseTrace(traceStream, treeRowid, timerStartTime, ttv, ids);
            }
        } finally {
            for (var tree : threads.values()) {
                tree.visitTimeAdvance(System.currentTimeMillis() - tree.getTime());
                tree.visitLabel(CallTreeRequest.DumperConstants.TAGS_CALL_ACTIVE, Value.str("HERE"));
                while (tree.getSp() > 0) {
                    tree.visitExit();
                }
                tree.visitEnd();
            }
        }

        return Pair.of(ids, threads);
    }

    void parseTrace(DataInputStreamEx traceStream, TreeRowId treeRowid, long timerStartTime, TreeTraceBuilder ttv, BitSet tagIds) throws IOException {
        // ProfilerTimeoutHandler.checkTimeout();

        long realTime = traceStream.readLong(); // start time
        int eventTime = (int) (timerStartTime - realTime);
        for (int idx = 0; ; idx++) {
            int header = traceStream.read();
            int typ = header & 0x3;
            if (typ == CallTreeRequest.DumperConstants.EVENT_FINISH_RECORD)
                break;

            int time = (header & 0x7f) >> 2;
            if ((header & 0x80) > 0)
                time |= traceStream.readVarInt() << 5;
            eventTime += time;

            boolean skipClob = idx < treeRowid.recordIndex;

            int tagId = 0;
            Value value = null;
            if (typ != CallTreeRequest.DumperConstants.EVENT_EXIT_RECORD) {
                tagId = traceStream.readVarInt();
                if (typ == CallTreeRequest.DumperConstants.EVENT_TAG_RECORD) {
                    value = readParameterValue(traceStream, !skipClob);
                }
            }
            if (skipClob) {
                continue;
            }

            long eventRealTime = eventTime + realTime;
            ttv.visitTimeAdvance(eventRealTime - ttv.getTime());
            tagIds.set(tagId);
            switch (typ) {
                case CallTreeRequest.DumperConstants.EVENT_ENTER_RECORD:
                    ttv.visitEnter(tagId);

                    break;
                case CallTreeRequest.DumperConstants.EVENT_EXIT_RECORD:
                    ttv.visitExit();
                    if (ttv.getSp() == 0) {
                        ttv.visitEnd();
                        return;
                    }
                    break;
                default:
                    if (value != null && !(tagId == 0 && value.isEmpty())) {
                        ttv.visitLabel(tagId, value, 0);
                    }
                    break;
            }
        }
    }

    private Value readParameterValue(DataInputStreamEx traceStream, boolean doClob) throws IOException {
        Value value = null;
        int paramType = traceStream.read();
        switch (paramType) {
            case ParamTypes.PARAM_INDEX:
            case ParamTypes.PARAM_INLINE:
                value = Value.str(traceStream.readString());
                break;
            case ParamTypes.PARAM_BIG_DEDUP:
            case ParamTypes.PARAM_BIG:
                int traceIndex = traceStream.readVarInt();
                int offs = traceStream.readVarInt();
                if (doClob) {
                    var clobType = paramType == ParamTypes.PARAM_BIG_DEDUP ? StreamType.SQL : StreamType.XML;
                    var newClob = Value.clob(podReference, clobType, traceIndex, offs);
                    value = clobIdx.getOrDefault(newClob.id(), newClob);
                } else {
                    Log.warnf("!!!");
                }
                break;
        }
        return value;
    }

}
