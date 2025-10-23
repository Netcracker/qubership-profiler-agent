package com.netcracker.cdt.ui.services.calls.search;

import com.netcracker.common.models.TimeRange;
import com.netcracker.profiler.model.CallFilterer;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.model.Call;
import com.netcracker.profiler.sax.call.CallDataReader;
import com.netcracker.profiler.sax.call.CallDataReaderFactory;
import com.netcracker.profiler.timeout.ReadInterruptedException;
import io.quarkus.logging.Log;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import static com.netcracker.common.Consts.CALL_HEADER_MAGIC;

/**
 * Parse Call (as raw entity) from `calls` stream.
 * <br>
 * Use `CallFilterer<Call>` to filter calls (by duration or parameter values) on early stage (not to spend memory during converting for UI)
 */
public class CallsExtractor {
    private final DataInputStreamEx calls;
    private final long begin, end;

    private int parsed;
    private int result;

    CallDataReader callDataReader;

    public CallsExtractor(DataInputStreamEx calls, TimeRange period) {
        this.calls = calls;
        this.begin = period.from().toEpochMilli();
        this.end = period.to().toEpochMilli();
    }

    public List<Call> findCallsInStream(String callsStreamIndex, final BitSet requiredIds, CallFilterer<Call> filterer, long endScan) throws IOException {
        var list = new ArrayList<Call>();
        try {
            CallsFileHeader cfh = readStartTime();
            int fileFormat = cfh.fileFormat();
            long time = cfh.startTime();

            callDataReader = CallDataReaderFactory.createReader(fileFormat);
            if (callDataReader == null) {
                Log.errorf("invalid calls' format %d in stream %s", fileFormat, callsStreamIndex);
                return list;
            }

            while (true) {
                if (Thread.interrupted()) {
                    throw new ReadInterruptedException();
                }

                Call call = new Call();
                callDataReader.read(call, calls);
                time += call.time;
                call.time = time;

                //since skipParams reads data anyways, it does not make much sense to skip populating these values
                //also rx-java-related parameters for aggregations are introduced in the list of params
                //we should not filter-out calls based on duration or callFilterer
                //however, need to skip calls based on the requested time range
                if ((call.time + call.duration < begin) || (call.time > end)) {
                    if (call.time > endScan) {
                        return list;
                    }
                    callDataReader.skipParams(call, calls);
                    continue;
                }

                call.setSuspendDuration(0); // TODO double check ("no suspend support"?)
//                call.setSuspendDuration(suspendLog.getSuspendDuration(call.time, call.time + call.duration));
                callDataReader.readParams(call, calls);

                parsed++;
                if (!filterer.filter(call)) {
                    continue; // skip call if it doesn't match criteria
                }
                requiredIds.set(call.method);
                if (call.params != null) {
                    for (var paramId : call.params.keySet()) {
                        requiredIds.set(paramId);
                    }
                }
                call.callsStreamIndex = callsStreamIndex;
                result++;
                list.add(call);
            }
        } catch (EOFException e) {
            // it's ok to get EOF when reading current stream
        }
        return list;
    }

    public int getParsedCount() {
        return parsed;
    }

    public int getResultCount() {
        return result;
    }

    private CallsFileHeader readStartTime() throws IOException {
        long time = calls.readLong();
        int fileFormat = 0;
        if ((int) (time >>> 32) == CALL_HEADER_MAGIC) {
            fileFormat = (int) (time & 0xffffffff);
            time = calls.readLong();
        }
        return new CallsFileHeader(fileFormat, time);
    }

    public record CallsFileHeader(int fileFormat, long startTime) {
    }

}
