package org.qubership.profiler.io;

import static org.qubership.profiler.util.ProfilerConstants.CALL_HEADER_MAGIC;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.io.call.CallDataReader;
import org.qubership.profiler.io.call.CallDataReaderFactory;
import org.qubership.profiler.io.call.ReactorCallReader;
import org.qubership.profiler.timeout.ReadInterruptedException;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class CallReader implements ICallReader {
    protected final CallFilterer cf;
    protected final CallListener callback;
    protected final Collection<Throwable> exceptions = new LinkedBlockingQueue<>();

    final static FileFilter CALLS_FILE_FINDER = new FileFilter() {
        public boolean accept(File pathname) {
            return (pathname.isDirectory() || pathname.getName().endsWith(".calls.log"));
        }
    };
    protected long callBeginTime;
    protected long minCallBeginTime;

    protected long begin = Long.MIN_VALUE;
    protected long end = Long.MAX_VALUE;
    protected long beginSuspendLog = Long.MIN_VALUE;
    protected long endSuspendLog = Long.MAX_VALUE;

    public Collection<Throwable> getExceptions() {
        return exceptions;
    }

    protected CallDataReader callDataReader;
    protected ReactorCallReader reactorCallReader;

    public CallReader(CallListener callback, CallFilterer cf) {
        this.callback = callback;
        this.cf = cf;
    }

    /**
     * Finds all the calls that match filter criteria
     */
    public final void find() {
        innerFind();
    }

    protected abstract void innerFind();

    public static CallsFileHeader readStartTime(DataInputStreamEx calls) throws IOException {
        long time = calls.readLong();
        int fileFormat = 0;
        if ((int) (time >>> 32) == CALL_HEADER_MAGIC) {
            fileFormat = (int) (time & 0xffffffff);
            time = calls.readLong();
        }
        return new CallsFileHeader(fileFormat, time);
    }

    protected boolean findCallsInStream(DataInputStreamEx is,
                                        DataInputStreamEx reactorCalls,
                                        String callsStreamIndex,
                                        SuspendLog suspendLog,
                                        ArrayList<Call> result,
                                        final BitSet requiredIds) {
        return findCallsInStream(is, reactorCalls, callsStreamIndex, suspendLog, result, requiredIds, Long.MAX_VALUE);
    }

    protected boolean findCallsInStream(DataInputStreamEx is,
                                     DataInputStreamEx reactorCalls,
                                     String callsStreamIndex,
                                     SuspendLog suspendLog,
                                     ArrayList<Call> result,
                                     final BitSet requiredIds,
                                     long endScan) {
        try {
            final DataInputStreamEx calls = is;
            final DataInputStreamEx rCalls = reactorCalls;
            CallsFileHeader cfh = readStartTime(is);
            int fileFormat = cfh.getFileFormat();
            long time = cfh.getStartTime();
            callBeginTime = time;
            minCallBeginTime = Math.min(minCallBeginTime, time);
            boolean reactorCallsAvailable = rCalls != null;

            CallDataReader reader = CallDataReaderFactory.createReader(fileFormat);
            callDataReader = reader;
            if (reactorCallsAvailable) {
                reactorCallReader = CallDataReaderFactory.createReactorReader(rCalls.readVarInt());
            }

            Call call = new Call();
            final long begin = this.begin;
            final long end = this.end;
            while (true) {
                if(Thread.interrupted()){
                    throw new ReadInterruptedException();
                }

                reader.read(call, calls, requiredIds);
                if (reactorCallsAvailable) {
                    reactorCallReader.read(call, rCalls);
                }

                time += call.time;
                call.time = time;

                //since skipParams reads data anyways, it does not make much sense to skip populating these values
                //also rx-java-related parameters for aggregations are introduced in the list of params
                //we should not filter-out calls based on duration or callFilterer
                //however, need to skip calls based on the requested time range
                if ((call.time + call.duration < begin) || (call.time > end)) {
                    if(call.time > endScan) {
                        return true;
                    }
                    reader.skipParams(call, calls);
                    continue;
                }

                call.setSuspendDuration(suspendLog.getSuspendDuration(call.time, call.time + call.duration));
                reader.readParams(call, calls, requiredIds);
                call.callsStreamIndex = callsStreamIndex;
                result.add(call);
                call = new Call();
            }
        } catch (EOFException e) {
            //it's ok to get EOF when reading current stream
        } catch (IOException e) {
            exceptions.add(e);
        }
        return false;
    }


    public void setTimeFilterCondition(long begin, long end) {
        this.begin = begin;
        this.end = end;
        this.beginSuspendLog = begin - (SUSPEND_LOG_READER_EXTRA_TIME * 60 * 1000);
        this.endSuspendLog = end + (SUSPEND_LOG_READER_EXTRA_TIME * 60 * 1000);
    }

    public long getBegin(){
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public String getRootReference(){
        return "unknown";
    }
}
