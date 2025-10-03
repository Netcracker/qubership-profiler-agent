package com.netcracker.profiler.sax.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.io.Call;
import com.netcracker.profiler.io.CallFilterer;
import com.netcracker.profiler.io.CallListener;
import com.netcracker.profiler.io.CallReader;
import com.netcracker.profiler.io.SuspendLog;

import org.junit.jupiter.api.Test;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

class MockCallReader extends CallReader {
    public MockCallReader(CallListener callback, CallFilterer cf) {
        super(callback, cf);
    }

    @Override
    protected void innerFind() {
    }

    public boolean findCalls(DataInputStreamEx is,
                             String callsStreamIndex,
            SuspendLog suspendLog,
            ArrayList<Call> result,
            final BitSet requiredIds,
            long endScan) {
        return findCallsInStream(is, callsStreamIndex, suspendLog, result, requiredIds, endScan);
    }
}

public class CallReaderTest {

    String getCallWithNonZeroValue(List<Call> result, String key) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
        Field field = result.get(0).getClass().getField(key);
        for (Call call : result) {
            Object value = field.get(call);
            String val = value.toString();
            if(!val.equals("0")) {
                return call.toString();
            }
        }
        return null;
    }

    @Test
    public void testfindCallsInStream() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        File file = ResourceUtils.getFile("classpath:storage/test_call.bin");
        DataInputStreamEx calls = DataInputStreamEx.openDataInputStream(file);
        MockCallReader spy = new MockCallReader(null, null);
        SuspendLog suspendLog = SuspendLog.EMPTY;
        ArrayList<Call> result = new ArrayList<Call>();
        BitSet requiredIds = new BitSet();
        long endScan = Long.MAX_VALUE;
        spy.findCalls(calls, null, suspendLog, result, requiredIds, endScan);
        assertEquals(27, result.size(), "Assertions No of Calls");
        assertEquals("Call{time=1691167327716, cpuTime=1184, waitTime=0, memoryUsed=0, method=9, duration=415, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=8, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='main', params=null}", result.get(0).toString(), "Assertions first Call");
        assertEquals("Call{time=1691167391053, cpuTime=37, waitTime=0, memoryUsed=50, method=0, duration=115, queueWaitDuration=51, suspendDuration=0, calls=0, traceFileIndex=0, bufferOffset=109, recordIndex=0, transactions=0, logsGenerated=101, logsWritten=0, fileRead=0, fileWritten=50, netRead=0, netWritten=37, threadName='unknown # 116', params=null}", result.get(result.size()-1).toString(), "Assertions last Call");
        assertEquals("Call{time=1691167330628, cpuTime=0, waitTime=0, memoryUsed=0, method=174, duration=1, queueWaitDuration=0, suspendDuration=0, calls=3, traceFileIndex=1, bufferOffset=997, recordIndex=11, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='background-preinit', params=null}", getCallWithNonZeroValue(result, "recordIndex"), "Assertions first non zero CallRecord for recordIndex");
        assertEquals("Call{time=1691167328131, cpuTime=10617, waitTime=0, memoryUsed=0, method=7, duration=26492, queueWaitDuration=0, suspendDuration=0, calls=631, traceFileIndex=1, bufferOffset=8, recordIndex=13, transactions=0, logsGenerated=0, logsWritten=0, fileRead=643898, fileWritten=6929, netRead=0, netWritten=0, threadName='main', params=null}", getCallWithNonZeroValue(result, "fileRead"), "Assertions first non zero CallRecord for fileRead");
        assertEquals("Call{time=1691167328131, cpuTime=10617, waitTime=0, memoryUsed=0, method=7, duration=26492, queueWaitDuration=0, suspendDuration=0, calls=631, traceFileIndex=1, bufferOffset=8, recordIndex=13, transactions=0, logsGenerated=0, logsWritten=0, fileRead=643898, fileWritten=6929, netRead=0, netWritten=0, threadName='main', params=null}", getCallWithNonZeroValue(result, "fileWritten"), "Assertions first non zero CallRecord for fileWritten");
        assertEquals("Call{time=1691167360335, cpuTime=249, waitTime=0, memoryUsed=0, method=555, duration=636, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=4320, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=118, netWritten=760, threadName='http-nio-8180-exec-1', params={84=[10.131.0.2], 41=[http://10.131.1.39:8180/actuator/health], 19=[GET /actuator/health, client: 10.131.0.2], 119=[GET]}}", getCallWithNonZeroValue(result, "netRead"), "Assertions first non zero CallRecord for netRead");
        assertEquals("Call{time=1691167360335, cpuTime=249, waitTime=0, memoryUsed=0, method=555, duration=636, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=4320, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=118, netWritten=760, threadName='http-nio-8180-exec-1', params={84=[10.131.0.2], 41=[http://10.131.1.39:8180/actuator/health], 19=[GET /actuator/health, client: 10.131.0.2], 119=[GET]}}", getCallWithNonZeroValue(result, "netWritten"), "Assertions first non zero CallRecord for netWritten");
        // assertNull(getCallWithNonZeroValue(result, "waitTime"), "Assertions first non zero CallRecord for waitTime");
        // assertNull(getCallWithNonZeroValue(result, "memoryUsed"), "Assertions first non zero CallRecord for memoryUsed");
        // assertNull(getCallWithNonZeroValue(result, "queueWaitDuration"), "Assertions first non zero CallRecord for queueWaitDuration");
        assertNull(getCallWithNonZeroValue(result, "suspendDuration"), "Assertions first non zero CallRecord for suspendDuration");
        // assertNull(getCallWithNonZeroValue(result, "transactions"), "Assertions first non zero CallRecord for transactions");
        // assertNull(getCallWithNonZeroValue(result, "logsGenerated"), "Assertions first non zero CallRecord for logsGenerated");
        // assertNull(getCallWithNonZeroValue(result, "logsWritten"), "Assertions first non zero CallRecord for logsWritten");
    }
}
