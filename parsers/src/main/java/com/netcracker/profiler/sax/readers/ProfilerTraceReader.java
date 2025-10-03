package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.configuration.ParameterInfoDto;
import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.ParamTypes;
import com.netcracker.profiler.io.ParamReader;
import com.netcracker.profiler.io.ParamReaderFactory;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.raw.*;
import com.netcracker.profiler.sax.values.ClobValue;
import com.netcracker.profiler.sax.values.StringValue;
import com.netcracker.profiler.sax.values.ValueHolder;
import com.netcracker.profiler.timeout.ProfilerTimeoutException;
import com.netcracker.profiler.timeout.ProfilerTimeoutHandler;
import com.netcracker.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public abstract class ProfilerTraceReader {
    public static final String TRACE_STREAM_NAME = "trace";

    private final static Logger log = LoggerFactory.getLogger(ProfilerTraceReader.class);

    protected final RepositoryVisitor rv;

    protected String rootReference;
    protected List<TreeRowid> treeRowids = new ArrayList<>();

    @Autowired
    protected ApplicationContext applicationContext;

    @Autowired
    protected ParamReaderFactory paramReaderFactory;

    public ProfilerTraceReader(RepositoryVisitor rv, String rootReference) {
        this.rv = rv;
        this.rootReference = rootReference;
    }

    public ProfilerTraceReader() {
        this(null, null);
        throw new RuntimeException("No-args not supported");
    }

    public interface DumperConstants {
        byte EVENT_EMPTY = -1;
        byte EVENT_ENTER_RECORD = 0;
        byte EVENT_EXIT_RECORD = 1;
        byte EVENT_TAG_RECORD = 2;
        byte EVENT_FINISH_RECORD = 3;
        byte COMMAND_ROTATE_LOG = 1;
        byte COMMAND_FLUSH_LOG = 2;
        byte COMMAND_EXIT = 3;

        int TAGS_ROOT = -1;
        int TAGS_HOTSPOTS = -2;
        int TAGS_PARAMETERS = -3;
        int TAGS_CALL_ACTIVE = -4;
    }

    public enum ClobReadMode {
        ALL_VALUES, FIRST_ONLY, LAST_ONLY, FIRST_AND_LAST
    }

    public enum ClobReadTypes {
        ALL_VALUES, XML_ONLY, SQL_ONLY
    }

    public abstract DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String streamName, int traceFileIndex) throws IOException;

    public void read(List<TreeRowid> treeRowids) {
        read(treeRowids, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @SuppressWarnings("resource")
    public void read(List<TreeRowid> treeRowids, long begin, long end) {
        boolean isTraceEnabled = log.isTraceEnabled();
        Collections.sort(treeRowids);

        this.treeRowids.addAll(treeRowids);

        final Map<Long, TreeTraceVisitor> threads = new HashMap<Long, TreeTraceVisitor>();

        DataInputStreamEx trace = null;
        readSuspendLog(begin, end);
//        String dataFolderPath = dataFolder.getPath();

        TraceVisitor tv = null;
        BitSet ids = new BitSet();
        HashMap<ClobValue, ClobValue> uniqueClobs = new HashMap<ClobValue, ClobValue>();
        try {
            tv = rv.visitTrace();

            int traceFileIndex = treeRowids.get(0).traceFileIndex;
            int callPos = 0;

            trace = reopenDataInputStream(trace, TRACE_STREAM_NAME, traceFileIndex);
            long timerStartTime = trace.readLong();

            MEGALOOP:
            while (callPos < treeRowids.size() || !threads.isEmpty()) {
                ProfilerTimeoutHandler.checkTimeout();
                int tracePos = trace.position();
                if (isTraceEnabled) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("New buffer. position: ").append(tracePos).append(", callPos: ").append(callPos);
                    if (callPos < treeRowids.size()) {
                        sb.append(", treeRowids[callPos].bufferOffset==").append(treeRowids.get(callPos).bufferOffset);
                    }
                    sb.append(", threads.size: ").append(threads.size());
                    log.trace(sb.toString());
                }
                if (threads.isEmpty() && callPos < treeRowids.size()) {
                    TreeRowid treeRowid = treeRowids.get(callPos);

                    if (traceFileIndex != treeRowid.traceFileIndex) {
                        traceFileIndex = treeRowid.traceFileIndex;
                        trace = reopenDataInputStream(trace, TRACE_STREAM_NAME, traceFileIndex);
                        timerStartTime = trace.readLong();
                        tracePos = trace.position();
                        if (isTraceEnabled)
                            log.trace("Opened new trace file " + rootReference + ", timerStartTime: " + timerStartTime + " (" + new Date(timerStartTime) + "), tracePos: " + tracePos);
                    }


                    if (tracePos < treeRowids.get(callPos).bufferOffset) {
                        if (isTraceEnabled) {
                            log.trace("Current offset {}, is less than required {}", tracePos, treeRowids.get(callPos).bufferOffset);
                        }
                        trace.skipBytes(treeRowids.get(callPos).bufferOffset - tracePos);
                        tracePos = trace.position();
                    }

                }
                Long currentThreadId;
                try {
                    currentThreadId = trace.readLong();
                    if (isTraceEnabled)
                        log.trace("currentThreadId: {}", currentThreadId);
                } catch (EOFException eof) {
                    traceFileIndex++;
                    try {
                        trace = reopenDataInputStream(trace, TRACE_STREAM_NAME, traceFileIndex);
                        if (trace == null) {
                            //also break mega loop. In cassandra-based implementation contract is to return null instead of EOF
                            break;
                        }
                    } catch (FileNotFoundException e) {
                        // Might happen when call did not finish yet
                        // We just stop reading at this point
                        break; // breaks MEGALOOP
                    }
                    timerStartTime = trace.readLong();
                    tracePos = trace.position();
                    currentThreadId = trace.readLong();
                    if (isTraceEnabled)
                        log.trace("Opened new trace file " + rootReference + ", timerStartTime: " + timerStartTime + " (" + new Date(timerStartTime) + "), tracePos: " + tracePos + "currentThreadId: " + currentThreadId);
                }

                TreeTraceVisitor ttv = threads.get(currentThreadId);
                boolean started = true;
                int startIndex = 0;
                if (ttv == null
                        && callPos < treeRowids.size()
                        && treeRowids.get(callPos).bufferOffset == tracePos
                        && treeRowids.get(callPos).traceFileIndex == traceFileIndex
                ) {
                    started = false;
                    TreeRowid treeRowid = treeRowids.get(callPos);
                    startIndex = treeRowid.recordIndex;
                    String fullRowId = treeRowid.fullRowId;
                    int folderId = treeRowid.folderId;
                    callPos++;
                    TreeRowid rowid = new TreeRowid(folderId, fullRowId, traceFileIndex, tracePos, startIndex);
                    threads.put(currentThreadId, ttv = tv.visitTree(rowid)); // TODO: pass suspendLog
                }
                long realTime = trace.readLong(); // start time
                int realTimeOffset = (int) (realTime - timerStartTime);
                if (isTraceEnabled)
                    log.trace("realTime: {}, realTimeOffset: {}", realTime, realTimeOffset);

                int eventTime = -realTimeOffset;
                for (int idx = 0; ; idx++) {
                    int header = trace.read();
                    int typ = header & 0x3;
                    if (typ == DumperConstants.EVENT_FINISH_RECORD)
                        break;

                    int time = (header & 0x7f) >> 2;
                    if ((header & 0x80) > 0)
                        time |= trace.readVarInt() << 5;
                    eventTime += time;

                    int tagId = 0;

                    ValueHolder value = null;
                    if (typ != DumperConstants.EVENT_EXIT_RECORD) {
                        tagId = trace.readVarInt();

                        if (typ == DumperConstants.EVENT_TAG_RECORD) {
                            int paramType = trace.read();
                            switch (paramType) {
                                case ParamTypes.PARAM_INDEX:
                                case ParamTypes.PARAM_INLINE:
                                    value = new StringValue(trace.readString());
                                    break;
                                case ParamTypes.PARAM_BIG_DEDUP:
                                case ParamTypes.PARAM_BIG:
                                    int traceIndex = trace.readVarInt();
                                    int offs = trace.readVarInt();
                                    if (ttv != null && started) {
                                        String clobFolder = paramType == ParamTypes.PARAM_BIG_DEDUP ? "sql" : "xml";
                                        ClobValue newClob = new ClobValue(rootReference, clobFolder, traceIndex, offs);
                                        value = newClob;
                                        ClobValue existingClob = uniqueClobs.get(newClob);
                                        if (existingClob != null) {
                                            value = existingClob;
                                        } else {
                                            uniqueClobs.put(newClob, newClob);
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                    if (ttv == null || (!started && idx < startIndex)) {
//                        System.out.println("ttv == null :" + (ttv == null));
//                        System.out.println("started && idx < startIndex" + (started && idx < startIndex) + " startIndex " + startIndex);
//                        System.out.println("idx: " + idx + ", eventTime: " + eventTime + ", ignoring typ: " + typ + ", tag " + value + " (" + tagId + ")");
//                        if (isTraceEnabled)
//                            log.trace("idx: " + idx + ", eventTime: " + eventTime + ", ignoring typ: " + typ + ", tag " + tag + " (" + tagId + ")");
                        continue;
                    }
                    started = true;

                    long eventRealTime = eventTime + realTime;
                    if (tagId == 1042 && (eventRealTime == 1405085865470L
                            || eventRealTime == 1405086718993L))
                        break MEGALOOP;
                    ttv.visitTimeAdvance(eventRealTime - ttv.getTime());
                    ids.set(tagId);
                    switch (typ) {
                        case DumperConstants.EVENT_ENTER_RECORD:
                            ttv.visitEnter(tagId);
                            if (isTraceEnabled)
                                log.trace("> idx: " + idx + ", eventTime: " + eventTime + ", eventRealTime: " + eventRealTime + ", tag: " + tagId + ", " + ttv.getSp());
                            break;
                        case DumperConstants.EVENT_EXIT_RECORD:
                            if (isTraceEnabled)
                                log.trace("< idx: " + idx + ", eventTime: " + eventTime + ", eventRealTime: " + eventRealTime + ", sp: " + ttv.getSp());
                            ttv.visitExit();
                            if (ttv.getSp() == 0) {
                                ttv.visitEnd();
                                if (callPos < treeRowids.size() && treeRowids.get(callPos).bufferOffset == tracePos && treeRowids.get(callPos).traceFileIndex == traceFileIndex) {
                                    started = false;
                                    TreeRowid treeRowid = treeRowids.get(callPos);
                                    startIndex = treeRowid.recordIndex;
                                    String fullRowId = treeRowid.fullRowId;
                                    int folderId = treeRowid.folderId;
                                    callPos++;
                                    TreeRowid rowid = new TreeRowid(folderId, fullRowId, traceFileIndex, tracePos, startIndex);
                                    threads.put(currentThreadId, ttv = tv.visitTree(rowid)); // TODO: pass suspendLog
                                } else {
//                                    System.out.println("-------end-------");
//                                    System.out.println("callPos < treeRowids.size() : " + (callPos < treeRowids.size()));
//                                    if (callPos < treeRowids.size()) {
//                                        System.out.println("treeRowids.get(callPos).bufferOffset :" + treeRowids.get(callPos).bufferOffset + " tracePos : " + tracePos + " " + trace.position());
//                                        System.out.println("treeRowids.get(callPos).traceFileIndex == traceFileIndex : " + (treeRowids.get(callPos).traceFileIndex == traceFileIndex));
//                                    }
//                                    System.out.println("--------------------------");
                                    threads.remove(currentThreadId);
                                    ttv = null;
                                    if (threads.isEmpty() && callPos == treeRowids.size()) break MEGALOOP;
                                }
                            }
                            break;
                        default:
                            if (value != null && !(tagId == 0 && value instanceof StringValue && ((StringValue) value).value.length() == 0)) {
                                ttv.visitLabel(tagId, value);
                                if (isTraceEnabled)
                                    log.trace("! idx: " + idx + ", eventTime: " + eventTime + ", eventRealTime: " + eventRealTime + ", tag: " + tagId + ", value: " + value); //TODO +", sp: " + ttc.sp);
                            }
                            break;
                    }
                }
            }
        } catch (Error | ProfilerTimeoutException e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().error("Error while reading profiling tree from folder " + rootReference
                    + ", rowids " + treeRowids.toString(), t);
        } finally {
            for (TreeTraceVisitor tree : threads.values()) {
                tree.visitTimeAdvance(System.currentTimeMillis() - tree.getTime());
                tree.visitLabel(DumperConstants.TAGS_CALL_ACTIVE, new StringValue("HERE"));
                while (tree.getSp() > 0) {
                    tree.visitExit();
                }
                tree.visitEnd();
            }
            if (tv != null)
                tv.visitEnd();
        }
        readDictionary(ids);

        readClobs(uniqueClobs.keySet());
    }

    public Set<ClobValue> readClobIds(File file, ClobReadMode mode, ClobReadTypes readTypes) {
        return readClobIdsOnly(file, mode, readTypes);
    }

    public static Set<ClobValue> readClobIdsOnly(File file, ClobReadMode mode, ClobReadTypes readTypes) {
        if (mode == null) mode = ClobReadMode.ALL_VALUES;
        if (readTypes == null) readTypes = ClobReadTypes.ALL_VALUES;

        DataInputStreamEx trace = null;
        String dataFolderPath = file.getParentFile().getParent();

        Set<ClobValue> uniqueClobIds = new LinkedHashSet<>();
        ClobValue lastClobId = null;
        boolean hasReadFirstValue = false;
        try {

            trace = DataInputStreamEx.openDataInputStream(file);
            trace.readLong(); //timerStartTime

            MEGALOOP:
            while (true) {
                trace.readLong(); //currentThreadId
                trace.readLong(); // start time

                for (int idx = 0; ; idx++) {
                    int header = trace.read();
                    int typ = header & 0x3;
                    if (typ == DumperConstants.EVENT_FINISH_RECORD)
                        break;

                    if ((header & 0x80) > 0)
                        trace.readVarInt(); //time

                    int tagId = 0;
                    if (typ != DumperConstants.EVENT_EXIT_RECORD) {
                        tagId = trace.readVarInt();
                        if (typ == DumperConstants.EVENT_TAG_RECORD) {
                            int paramType = trace.read();
                            switch (paramType) {
                                case ParamTypes.PARAM_INDEX:
                                case ParamTypes.PARAM_INLINE:
                                    trace.readString();
                                    break;
                                case ParamTypes.PARAM_BIG_DEDUP:
                                case ParamTypes.PARAM_BIG:
                                    int traceIndex = trace.readVarInt();
                                    int offs = trace.readVarInt();
                                    if ((readTypes == ClobReadTypes.XML_ONLY && paramType == ParamTypes.PARAM_BIG) ||
                                            (readTypes == ClobReadTypes.SQL_ONLY && paramType == ParamTypes.PARAM_BIG_DEDUP) ||
                                            readTypes == ClobReadTypes.ALL_VALUES) {

                                        String clobFolder = paramType == ParamTypes.PARAM_BIG_DEDUP ? "sql" : "xml";
                                        ClobValue newClob = new ClobValue(dataFolderPath, clobFolder, traceIndex, offs);

                                        if (mode == ClobReadMode.FIRST_ONLY) {
                                            uniqueClobIds.add(newClob);
                                            break MEGALOOP;
                                        } else if (mode == ClobReadMode.LAST_ONLY) {
                                            lastClobId = newClob;
                                        } else if (mode == ClobReadMode.FIRST_AND_LAST) {
                                            if (!hasReadFirstValue) {
                                                uniqueClobIds.add(newClob);
                                                hasReadFirstValue = true;
                                            }
                                            lastClobId = newClob;
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
        } catch (EOFException eof) {
            //DoNothing
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().warn("Error while reading clobIds from folder " + dataFolderPath, t);
        } finally {
            IOHelper.close(trace);
        }
        if ((mode == ClobReadMode.LAST_ONLY || mode == ClobReadMode.FIRST_AND_LAST) && lastClobId != null) {
            uniqueClobIds.add(lastClobId);
        }

        return uniqueClobIds;
    }

    protected SuspendLogReader suspendLogReader(SuspendLogVisitor sv, long begin, long end) {
        return applicationContext.getBean(SuspendLogReader.class, sv, rootReference, begin, end);
    }

    protected SuspendLogReader suspendLogReader(SuspendLogVisitor sv) {
        return suspendLogReader(sv, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    protected void readSuspendLog(long begin, long end) {
        SuspendLogVisitor sv = rv.visitSuspendLog();
        if(sv == null) {
            return;
        }
        SuspendLogReader reader = suspendLogReader(sv, begin, end);
        reader.read();
    }

    protected void readSuspendLog() {
        readSuspendLog(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    protected ParamReader paramReader() {
        return paramReaderFactory.getInstance(rootReference);
    }

    protected void readDictionary(BitSet ids) {
        DictionaryVisitor dv = rv.visitDictionary();
        if(dv == null) {
            return;
        }
        boolean isTraceEnabled = log.isTraceEnabled();
        try {
            ParamReader paramReader = paramReader();
            Collection<Throwable> t = new ArrayList<Throwable>();
            List<String> tags = paramReader.fillTags(ids, t);

            for (int i = -1; (i = ids.nextSetBit(i + 1)) >= 0; ) {
                String s = tags.get(i);
                if (s == null)
                    continue;
                if (isTraceEnabled)
                    log.trace("Param: id={}, value={}", i, s);
                dv.visitName(i, s);
            }

            final Map<String, ParameterInfoDto> paramInfos = paramReader().fillParamInfo(t, rootReference);
            for (ParameterInfoDto info : paramInfos.values()) {
                if (isTraceEnabled)
                    log.trace("ParamInfo: {}", info);
                dv.visitParamInfo(info);
            }

        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().error("Unable to read dictionary from " + rootReference, t);
        }
        dv.visitEnd();
    }

    public abstract ClobReaderFlyweight clobReaderFlyweight();

    public void readClobs(Set<ClobValue> clobValueSet) {
        ClobValueVisitor cv = rv.visitClobValues();
        if (cv == null)
            return;

        try {
            ClobValue[] clobs = clobValueSet.toArray(new ClobValue[clobValueSet.size()]);
            Arrays.sort(clobs);

            ClobReaderFlyweight fw = clobReaderFlyweight();

            for (ClobValue clob : clobs) {
                fw.adaptTo(clob);
                cv.acceptValue(clob, fw);
            }
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().error("Unable to read clobs from " + rootReference, t);
        }
        cv.visitEnd();
    }
}
