package org.qubership.profiler.sax.readers;

import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.sax.builders.SuspendLogBuilder;

import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;

public class SuspendLogReaderTest {

    @Test
    public void testLogLoads() throws Exception {
        URL resource = getClass().getResource("/suspend_logs/1505741810159");
        File f = new File(resource.toURI());
        SuspendLogBuilder sb = new SuspendLogBuilder(2, 4, null);
        sb.initLog();
        SuspendLogReader sr = new SuspendLogReader(sb, f.getAbsolutePath());
        sr.read();
        SuspendLog sl = sb.get();
        SuspendLog.SuspendLogCursor cursor = sl.cursor();
        cursor.skipTo(0);
        int total = cursor.moveTo(System.currentTimeMillis());
        Assert.assertEquals("total suspend time", total, 1371777, 1000);
    }
}
