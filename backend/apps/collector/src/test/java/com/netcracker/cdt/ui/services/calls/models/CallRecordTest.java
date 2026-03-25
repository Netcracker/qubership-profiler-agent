package com.netcracker.cdt.ui.services.calls.models;

import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static com.netcracker.common.Consts.*;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class CallRecordTest {
    Instant T1 = Instant.parse("2023-07-01T10:00:00Z");
    PodIdRestart pod = PodIdRestart.of(PodName.of("test", "test", "pod"), T1);

    @Test
    void getAtIndex() {
        Instant t = Instant.parse("2023-07-01T10:00:00Z");
        assertEquals(1688205600000L, t.toEpochMilli());

        var pp = Utils.params(
                Utils.pv("test1", 1, 2, 4),
                Utils.pv("test2", 4, 5));

        var cr = new CallRecord(t.toEpochMilli(),
                1085, 100, 95, 1, 2,
                10, pod,"traceRecordId", "method", 12,
                1234000, 10000, 7000, 10, 7, 200000, 198000,
        pp);

        checkColumn(cr, C_TIME, 1688205599999L, false, true, true);
        checkColumn(cr, C_DURATION, 1086, true, false, true);
        checkColumn(cr, C_NON_BLOCKING, 100L, false,true,true);
        checkColumn(cr, C_CPU_TIME, 95L, false,true,true);
        checkColumn(cr, C_QUEUE_WAIT_TIME, 1, true, false, true);
        checkColumn(cr, C_SUSPENSION, 2, true, false, true);
        checkColumn(cr, C_CALLS, 10, true, false, true);
//        checkColumn(cr, C_FOLDER_ID, -10, false, false, false); // TODO: TBD
        checkColumn(cr, C_FOLDER_ID, "pod_1688205600000", true, false, true);
        checkColumn(cr, C_ROWID, "traceRecordId", false, false, false);
        checkColumn(cr, C_METHOD, "method", false, false, false); // literal's id
        checkColumn(cr, C_TRANSACTIONS, 12L, false, true, true);
        checkColumn(cr, C_MEMORY_ALLOCATED, 1234000L, false, true, true);
        checkColumn(cr, C_LOG_GENERATED, 10000, true, false, true);
        checkColumn(cr, C_LOG_WRITTEN, 7000, true, false, true);
        checkColumn(cr, C_FILE_TOTAL, 17L, false, true, true);
        checkColumn(cr, C_FILE_WRITTEN, 7L, false, true, true);
        checkColumn(cr, C_NET_TOTAL, 200000L + 198000, false, true, true);
        checkColumn(cr, C_NET_WRITTEN, 198000L, false, true, true);

        assertNull(cr.getAtIndex(18));
        assertNull(cr.getAtIndex(19));
    }

    private static <T> void checkColumn(CallRecord cr, int column, T expected,
                                        boolean isInt, boolean isLong, boolean comparable) {
        assertEquals(expected, cr.getAtIndex(column));
        assertEquals(isInt, CallRecord.isInt(column));
        assertEquals(isLong, CallRecord.isLong(column));
        assertEquals(comparable, CallRecord.isComparable(column));
    }

    @Test
    void sort() {
        Instant t1 = Instant.parse("2023-07-01T10:00:00Z");
        Instant t2 = Instant.parse("2023-07-01T10:01:00Z");

        var pp = Utils.params(
                Utils.pv("test1", 1, 2, 4),
                Utils.pv("test2", 4, 5));

        var cr1 = new CallRecord(t1.toEpochMilli(),
                1001, 100, 95, 1, 2,
                10, pod, "traceRecordId1", "method", 12,
                1234000, 10000, 7000, 10, 7, 200000, 198000,
                pp);
        var cr2 = new CallRecord(t2.toEpochMilli(),
                1002, 100, 95, 1, 2,
                10, pod, "traceRecordId2", "method", 12,
                1234000, 10000, 7000, 10, 7, 200000, 198000,
                pp);

        var ts = sorted(0, true, cr1, cr2);

        assertEquals(1688205599999L, ts.get(0).getAtIndex(C_TIME));
        assertEquals(1002, ts.get(0).getAtIndex(C_DURATION));
        assertEquals("pod_1688205600000", ts.get(0).getAtIndex(C_FOLDER_ID));
        assertEquals("traceRecordId1", ts.get(0).getAtIndex(C_ROWID));

        assertEquals(1688205659999L, ts.get(1).getAtIndex(C_TIME));
        assertEquals(1003, ts.get(1).getAtIndex(C_DURATION));
        assertEquals("pod_1688205600000", ts.get(1).getAtIndex(C_FOLDER_ID));
        assertEquals("traceRecordId2", ts.get(1).getAtIndex(C_ROWID));

        ts = sorted(0, true, cr2, cr1);
        assertEquals("traceRecordId1", ts.get(0).getAtIndex(C_ROWID));
        assertEquals("traceRecordId2", ts.get(1).getAtIndex(C_ROWID));

        ts = sorted(0, false, cr2, cr1);
        assertEquals("traceRecordId2", ts.get(0).getAtIndex(C_ROWID));
        assertEquals("traceRecordId1", ts.get(1).getAtIndex(C_ROWID));

        ts = sorted(1, true, cr2, cr1);
        assertEquals("traceRecordId1", ts.get(0).getAtIndex(C_ROWID));
        assertEquals("traceRecordId2", ts.get(1).getAtIndex(C_ROWID));

    }

    @Test
    void copy() {
        Instant t = Instant.parse("2023-07-01T10:00:00Z");
        assertEquals(1688205600000L, t.toEpochMilli());

        var pm = Utils.pod("pod");
        pm.putLiteral(101, "method1");
        pm.putLiteral(102, "param1");
        pm.putLiteral(103, "param2");
        pm.putLiteral(104, "param3");
        pm.putLiteral(34, "val1");
        pm.putLiteral(45, "val2");
        pm.putLiteral(95, "val3");
        pm.putLiteral(55, "val4");

        pm.putParameter("param1", false, false, 10, "method1");
        pm.putParameter("param2", false, false, 20, "method2");
        pm.putParameter("param3", false, false, 30, "method3");
        var params = Map.of(102, List.of("val1", "val2"), 103, List.of("val2","val3"), 104, List.of("val4"));

        var c = Utils.originCall(t, 1085, 100, 95, 1, 2,
                10, 91, 92, 93, 101, 12,
                1234000, 10000, 7000, 10, 7, 200000, 198000,
                params);
//        var cr = CallConverter.convertCall(c, pm, new TreeSet<>());

        assertEquals(1, CallConverter.convert(Stream.of(c), pm).toList().size());

    }

    private static List<CallRecord> sorted(int sortIndex, boolean asc, CallRecord... c) {
        var ts = new ArrayList<CallRecord>(c.length);
        ts.addAll(Arrays.asList(c));
        Collections.sort(ts, CallRecord.comparator(sortIndex, asc));
        return ts;
    }

}