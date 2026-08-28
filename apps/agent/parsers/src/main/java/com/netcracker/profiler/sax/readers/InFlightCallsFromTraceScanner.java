package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.ParamTypes;
import com.netcracker.profiler.guice.DumpRootLocation;
import com.netcracker.profiler.io.ParamReader;
import com.netcracker.profiler.io.ParamReaderFileFactory;

import com.google.inject.assistedinject.Assisted;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import jakarta.inject.Inject;

public class InFlightCallsFromTraceScanner {
    public static final String TRACE_STREAM_NAME = "trace";

    private final File dumpRoot;
    private final ParamReaderFileFactory paramReaderFileFactory;

    private final String rootReference;
    private final int startFileIndex;

    public static class CallInfo {
        public int traceFileIndex;
        public int bufferOffset;
        public int recordIndex;
        public int sp;
        public int methodId;
        public String methodName;
        public long startTime;
        public int duration;

        public CallInfo(int traceFileIndex, int bufferOffset, int recordIndex) {
            this.traceFileIndex = traceFileIndex;
            this.bufferOffset = bufferOffset;
            this.recordIndex = recordIndex;
        }

        public CallInfo() {
        }
    }

    @Inject
    public InFlightCallsFromTraceScanner(
            @DumpRootLocation File dumpRoot,
            ParamReaderFileFactory paramReaderFileFactory,
            @Assisted("rootReference") String rootReference,
            @Assisted("startFileIndex") int startFileIndex) {
        this.dumpRoot = dumpRoot;
        this.paramReaderFileFactory = paramReaderFileFactory;
        this.rootReference = rootReference;
        this.startFileIndex = startFileIndex;
    }

    private File dataFolder() {
        return new File(dumpRoot, rootReference);
    }

    public DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String streamName, int traceFileIndex) throws IOException {
        return DataInputStreamEx.reopenDataInputStream(oldOne, dataFolder(), streamName, traceFileIndex);
    }

    public List<CallInfo> find() throws IOException {
        final Map<Long, CallInfo> threads = new HashMap<>();

        DataInputStreamEx trace = null;
        int traceFileIndex = startFileIndex;

        trace = reopenDataInputStream(trace, TRACE_STREAM_NAME, traceFileIndex);
        long timerStartTime = trace.readLong();
        long maxEventRealTime = 0;

        MEGALOOP:
        while (true) {
            int tracePos = trace.position();
            Long currentThreadId;
            try {
                currentThreadId = trace.readLong();

                CallInfo ci = threads.get(currentThreadId);
                if (ci == null) {
                    threads.put(currentThreadId, ci = new CallInfo(traceFileIndex, tracePos, 0));
                }
                long realTime = trace.readLong(); // start time
                int realTimeOffset = (int) (realTime - timerStartTime);

                int eventTime = -realTimeOffset;
                for (int idx = 0; ; idx++) {
                    int header = trace.read();
                    int typ = header & 0x3;
                    if (typ == ProfilerTraceReader.DumperConstants.EVENT_FINISH_RECORD)
                        break;

                    int time = (header & 0x7f) >> 2;
                    if ((header & 0x80) > 0)
                        time |= trace.readVarInt() << 5;
                    eventTime += time;

                    int tagId = 0;
                    if (typ != ProfilerTraceReader.DumperConstants.EVENT_EXIT_RECORD) {
                        tagId = trace.readVarInt();

                        if (typ == ProfilerTraceReader.DumperConstants.EVENT_TAG_RECORD) {
                            int paramType = trace.read();
                            switch (paramType) {
                                case ParamTypes.PARAM_INDEX:
                                case ParamTypes.PARAM_INLINE:
                                    trace.skipString();
                                    break;
                                case ParamTypes.PARAM_BIG_DEDUP:
                                case ParamTypes.PARAM_BIG:
                                    trace.readVarInt();
                                    trace.readVarInt();
                                    break;
                            }
                        }
                    }

                    long eventRealTime = eventTime + realTime;
                    if (eventRealTime > maxEventRealTime) {
                        maxEventRealTime = eventRealTime;
                    }
                    if (tagId == 1042 && (eventRealTime == 1405085865470L
                            || eventRealTime == 1405086718993L))
                        break MEGALOOP;
                    switch (typ) {
                        case ProfilerTraceReader.DumperConstants.EVENT_ENTER_RECORD:
                            if (ci.sp <= 0) {
                                ci.traceFileIndex = traceFileIndex;
                                ci.bufferOffset = tracePos;
                                ci.recordIndex = idx;
                                ci.sp = 1;
                                ci.methodId = tagId;
                                ci.startTime = eventRealTime;
                            } else {
                                ci.sp++;
                            }
                            break;
                        case ProfilerTraceReader.DumperConstants.EVENT_EXIT_RECORD:
                            ci.sp--;
                    }
                }
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
            }
        }

        return buildResultCallsList(threads.values(), maxEventRealTime);
    }

    private List<CallInfo> buildResultCallsList(Collection<CallInfo> threads, long lastTimestamp) {
        List<CallInfo> calls = new ArrayList<>();
        BitSet ids = new BitSet();
        for (CallInfo c : threads) {
            if (c.sp <= 0) {
                continue;
            }
            calls.add(c);
            c.duration = (int) (lastTimestamp - c.startTime);
            ids.set(c.methodId);
        }
        ParamReader paramReader = paramReaderFileFactory.create(dataFolder());
        List<String> tags = paramReader.fillTags(ids, new ArrayList<Throwable>()).asList();
        for (CallInfo c : calls) {
            c.methodName = tags.get(c.methodId);
        }

        return calls;
    }
}
