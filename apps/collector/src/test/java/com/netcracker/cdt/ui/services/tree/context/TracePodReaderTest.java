package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.cdt.ui.services.tree.context.TracePodReader;
import com.netcracker.cdt.ui.services.tree.context.TreeTraceBuilder;
import com.netcracker.profiler.model.TreeRowId;
import com.netcracker.utils.UnitTest;
import com.netcracker.utils.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.BitSet;

import static com.netcracker.utils.Utils.setOf;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class TracePodReaderTest {

    @Test
    void parseTrace() throws IOException {
        var r = new TracePodReader("test");
        var is = Utils.testRawDataStream("binary/u5min-service.traces.0.bin");
        TreeRowId rowId = new TreeRowId(1, "1_1_8_0_0_0", 1, 8, 1);

        var ttv = TreeTraceBuilder.create(r.dictIdx, r.sRange, r.clobIdx, rowId);
        var tagIds = new BitSet();


        long timerStartTime = is.readLong();
        assertEquals(1691167326615L, timerStartTime);
        long currentThreadId = is.readLong();
        assertEquals(1L, currentThreadId);
        r.parseTrace(is, rowId, timerStartTime, ttv, tagIds);

        assertEquals(setOf(0, 33, 35), tagIds);
        assertEquals(1691167327796L, ttv.getTime());

    }
}