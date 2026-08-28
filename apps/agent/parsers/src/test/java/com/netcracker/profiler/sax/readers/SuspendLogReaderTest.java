package com.netcracker.profiler.sax.readers;

import static org.junit.jupiter.api.Assertions.*;

import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.builders.SuspendLogBuilder;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

public class SuspendLogReaderTest {

    @Test
    public void testLogLoads() throws Exception {
        URL resource = getClass().getResource("/suspend_logs/1505741810159");
        File f = new File(resource.toURI());
        SuspendLogBuilder sb = new SuspendLogBuilder(2, 4, null);
        SuspendLogReader sr = new SuspendLogReader(sb, f.getAbsolutePath());
        sr.read();
        SuspendLog sl = sb.get();
        SuspendLog.SuspendLogCursor cursor = sl.cursor();
        cursor.skipTo(0);
        int total = cursor.moveTo(System.currentTimeMillis());
        if (Math.abs(total - 1371777) >= 1000) {
            fail("total suspend time should be 1371777 +- 1000 ms, but is " + total);
        }
    }
}
