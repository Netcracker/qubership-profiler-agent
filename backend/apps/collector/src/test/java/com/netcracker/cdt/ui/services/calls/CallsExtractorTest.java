package com.netcracker.cdt.ui.services.calls;

import com.netcracker.cdt.ui.services.calls.search.CallsExtractor;
import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.profiler.model.Call;
import com.netcracker.utils.UnitTest;
import io.quarkus.logging.Log;
import org.jboss.logmanager.Level;
import org.jboss.logmanager.LogContext;
import org.jboss.logmanager.formatters.PatternFormatter;
import org.jboss.logmanager.handlers.ConsoleHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.netcracker.utils.Utils.testZipDataStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

@UnitTest
class CallsExtractorTest {

    @BeforeAll
    static void init() {
        var logFormat = "[%d{yyyy-MM-dd'T'HH:mm:ss.SSS}] [%p] [class=%c{2.}] %s%e%n";
        var handler = new ConsoleHandler(new PatternFormatter(logFormat));
        handler.setLevel(Level.TRACE);
        LogContext.getLogContext().getLogger("").addHandler(handler);
    }

    @Test
    void findCallsInStream() {
        try (var dis = testZipDataStream("binary/test14.calls.bin")) {
            var ext = new CallsExtractor(dis, TimeRange.ofEpochMilli(0, 1689422758000L));
            var requiredTagIds = new BitSet();
            var filterer = new InternalCallFilter(DurationRange.ofSeconds(0, 10000));

            // parse to non-enriched entity (Call)
            var list = ext.findCallsInStream("testStream", requiredTagIds, filterer, -1);
            Log.debugf("bits: %s \n", requiredTagIds.toString());
            Log.debugf("calls: %s \n", list.size());
            for (Call c : list) {
                Log.debugf("call#: %s \n", c.toString());
            }

            var requiredIds = requiredTagIds.stream().boxed().collect(Collectors.toList());
            assertEquals(14, list.size());
            assertEquals(List.of(7, 9, 19, 41, 84, 119, 168, 176, 565, 603), requiredIds);
            assertEquals(1689255943847L, list.get(0).time);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}

// Call{time=1689255943847, cpuTime=1484,  waitTime=0, memoryUsed=0, method=9,   duration=1313,  queueWaitDuration=0, suspendDuration=0, calls=4,   traceFileIndex=1, bufferOffset=8,    recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='main', params=null}
// Call{time=1689255995805, cpuTime=55,    waitTime=0, memoryUsed=0, method=176, duration=57,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=2236, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='Notification Thread', params=null}
// Call{time=1689255989375, cpuTime=0,     waitTime=0, memoryUsed=0, method=176, duration=69,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=2447, recordIndex=3,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='s0-admin-0', params=null}
// Call{time=1689255995651, cpuTime=255,   waitTime=0, memoryUsed=0, method=176, duration=48,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=2447, recordIndex=48, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=473, netRead=0, netWritten=0, threadName='s0-admin-0', params=null}
// Call{time=1689255995743, cpuTime=111,   waitTime=0, memoryUsed=0, method=176, duration=327,   queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=2447, recordIndex=56, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0, netRead=0, netWritten=0, threadName='s0-admin-0', params=null}
// Call{time=1689255997254, cpuTime=0,     waitTime=0, memoryUsed=0, method=168, duration=89,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=3003, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=144, netRead=0, netWritten=0, threadName='s1-admin-0', params=null}
// Call{time=1689255995456, cpuTime=17,    waitTime=0, memoryUsed=0, method=168, duration=87,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=3161, recordIndex=12, transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=252, netRead=9, netWritten=53, threadName='s0-io-1', params=null}
// Call{time=1689255997952, cpuTime=0,     waitTime=0, memoryUsed=0, method=603, duration=93,    queueWaitDuration=0, suspendDuration=0, calls=1,   traceFileIndex=1, bufferOffset=3640, recordIndex=6,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=252, netRead=207, netWritten=181, threadName='s1-io-0', params=null}
// Call{time=1689255945160, cpuTime=13576, waitTime=0, memoryUsed=0, method=7,   duration=93790, queueWaitDuration=0, suspendDuration=0, calls=619, traceFileIndex=1, bufferOffset=8,    recordIndex=13, transactions=0, logsGenerated=0, logsWritten=0, fileRead=643899, fileWritten=8802, netRead=0, netWritten=0, threadName='main', params=null}
// Call{time=1689256038753, cpuTime=233,   waitTime=0, memoryUsed=0, method=565, duration=2104,  queueWaitDuration=0, suspendDuration=0, calls=3,   traceFileIndex=1, bufferOffset=5215, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0,   netRead=121, netWritten=759,   threadName='http-nio-8180-exec-1', params={84=[192.168.206.37], 41=[http://10.131.130.142:8180/actuator/health], 19=[GET /actuator/health, client: 192.168.206.37], 119=[GET]}}
// Call{time=1689256038753, cpuTime=251,   waitTime=0, memoryUsed=0, method=565, duration=2105,  queueWaitDuration=0, suspendDuration=0, calls=5,   traceFileIndex=1, bufferOffset=5679, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=429, netRead=121, netWritten=759,   threadName='http-nio-8180-exec-2', params={84=[192.168.206.37], 41=[http://10.131.130.142:8180/actuator/health], 19=[GET /actuator/health, client: 192.168.206.37], 119=[GET]}}
// Call{time=1689256048571, cpuTime=0,     waitTime=0, memoryUsed=0, method=565, duration=75,    queueWaitDuration=0, suspendDuration=0, calls=3,   traceFileIndex=1, bufferOffset=6162, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0,   netRead=121, netWritten=759,   threadName='http-nio-8180-exec-4', params={84=[192.168.206.37], 41=[http://10.131.130.142:8180/actuator/health], 19=[GET /actuator/health, client: 192.168.206.37], 119=[GET]}}
// Call{time=1689256048286, cpuTime=54,    waitTime=0, memoryUsed=0, method=565, duration=173,   queueWaitDuration=0, suspendDuration=0, calls=4,   traceFileIndex=1, bufferOffset=6613, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0,   netRead=212, netWritten=98392, threadName='http-nio-8180-exec-3', params={84=[10.131.130.167], 41=[http://10.131.130.142:8180/actuator/prometheus], 19=[GET /actuator/prometheus, client: 10.131.130.167], 119=[GET]}}
// Call{time=1689256048571, cpuTime=0,     waitTime=0, memoryUsed=0, method=565, duration=6,     queueWaitDuration=0, suspendDuration=0, calls=3,   traceFileIndex=1, bufferOffset=7096, recordIndex=0,  transactions=0, logsGenerated=0, logsWritten=0, fileRead=0, fileWritten=0,   netRead=121, netWritten=759,   threadName='http-nio-8180-exec-5', params={84=[192.168.206.37], 41=[http://10.131.130.142:8180/actuator/health], 19=[GET /actuator/health, client: 192.168.206.37], 119=[GET]}}
