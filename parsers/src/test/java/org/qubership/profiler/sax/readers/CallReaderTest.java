package org.qubership.profiler.sax.readers;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.io.Call;
import org.qubership.profiler.io.CallFilterer;
import org.qubership.profiler.io.CallListener;
import org.qubership.profiler.io.CallReader;
import org.qubership.profiler.io.SuspendLog;

import org.junit.Assert;
import org.junit.Test;
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
            DataInputStreamEx reactorCalls,
            String callsStreamIndex,
            SuspendLog suspendLog,
            ArrayList<Call> result,
            final BitSet requiredIds,
            long endScan) {
        return findCallsInStream(is, reactorCalls, callsStreamIndex, suspendLog, result, requiredIds, endScan);
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
        spy.findCalls(calls, null, null, suspendLog, result, requiredIds, endScan);
        Assert.assertEquals("Assert No of Calls", 422, result.size());
        Assert.assertEquals("Assert first Call", "Call{time=1691167327716, cpuTime=1184, waitTime=0, memoryUsed=0, method=9, duration=415, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=8, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='main', params=null}", result.get(0).toString());
        Assert.assertEquals("Assert last Call", "Call{time=1691167680317, cpuTime=4, waitTime=0, memoryUsed=0, method=555, duration=3, queueWaitDuration=0, suspendDuration=0, calls=3, traceFileIndex=1, bufferOffset=447788, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=118, netWritten=760, threadName='http-nio-8180-exec-6', params={84=[10.131.0.2], 41=[http://10.131.1.39:8180/actuator/health], 19=[GET /actuator/health, client: 10.131.0.2], 119=[GET]}}", result.get(result.size()-1).toString());
        Assert.assertEquals("Assert first non zero CallRecord for recordIndex","Call{time=1691167330628, cpuTime=0, waitTime=0, memoryUsed=0, method=174, duration=1, queueWaitDuration=0, suspendDuration=0, calls=3, traceFileIndex=1, bufferOffset=997, recordIndex=11, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='background-preinit', params=null}", getCallWithNonZeroValue(result, "recordIndex"));
        Assert.assertEquals("Assert first non zero CallRecord for fileRead","Call{time=1691167328131, cpuTime=10617, waitTime=0, memoryUsed=0, method=7, duration=26492, queueWaitDuration=0, suspendDuration=0, calls=631, traceFileIndex=1, bufferOffset=8, recordIndex=13, transactions=0, logsGenerated=0, logsWritten=0, fileRead=643898, fileWritten=6929, netRead=0, netWritten=0, threadName='main', params=null}", getCallWithNonZeroValue(result, "fileRead"));
        Assert.assertEquals("Assert first non zero CallRecord for fileWritten","Call{time=1691167328131, cpuTime=10617, waitTime=0, memoryUsed=0, method=7, duration=26492, queueWaitDuration=0, suspendDuration=0, calls=631, traceFileIndex=1, bufferOffset=8, recordIndex=13, transactions=0, logsGenerated=0, logsWritten=0, fileRead=643898, fileWritten=6929, netRead=0, netWritten=0, threadName='main', params=null}", getCallWithNonZeroValue(result, "fileWritten"));
        Assert.assertEquals("Assert first non zero CallRecord for netRead","Call{time=1691167360335, cpuTime=249, waitTime=0, memoryUsed=0, method=555, duration=636, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=4320, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=118, netWritten=760, threadName='http-nio-8180-exec-1', params={84=[10.131.0.2], 41=[http://10.131.1.39:8180/actuator/health], 19=[GET /actuator/health, client: 10.131.0.2], 119=[GET]}}", getCallWithNonZeroValue(result, "netRead"));
        Assert.assertEquals("Assert first non zero CallRecord for netWritten","Call{time=1691167360335, cpuTime=249, waitTime=0, memoryUsed=0, method=555, duration=636, queueWaitDuration=0, suspendDuration=0, calls=4, traceFileIndex=1, bufferOffset=4320, recordIndex=0, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=118, netWritten=760, threadName='http-nio-8180-exec-1', params={84=[10.131.0.2], 41=[http://10.131.1.39:8180/actuator/health], 19=[GET /actuator/health, client: 10.131.0.2], 119=[GET]}}", getCallWithNonZeroValue(result, "netWritten"));
        Assert.assertNull("Assert first non zero CallRecord for waitTime", getCallWithNonZeroValue(result, "waitTime"));
        Assert.assertNull("Assert first non zero CallRecord for memoryUsed", getCallWithNonZeroValue(result, "memoryUsed"));
        Assert.assertNull("Assert first non zero CallRecord for queueWaitDuration", getCallWithNonZeroValue(result, "queueWaitDuration"));
        Assert.assertNull("Assert first non zero CallRecord for suspendDuration", getCallWithNonZeroValue(result, "suspendDuration"));
        Assert.assertNull("Assert first non zero CallRecord for transactions", getCallWithNonZeroValue(result, "transactions"));
        Assert.assertNull("Assert first non zero CallRecord for logsGenerated", getCallWithNonZeroValue(result, "logsGenerated"));
        Assert.assertNull("Assert first non zero CallRecord for logsWritten", getCallWithNonZeroValue(result, "logsWritten"));
    }
}
