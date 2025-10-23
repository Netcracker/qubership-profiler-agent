package com.netcracker.cdt.ui.services.calls;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.search.InternalCallFilter;
import com.netcracker.cdt.ui.services.calls.search.PodSequenceParser;
import com.netcracker.common.models.DurationRange;
import com.netcracker.cdt.ui.models.PodMetaData;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.common.models.TimeRange;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.netcracker.utils.Utils.*;
import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class PodSequenceCallParserTest {
    public static final String CALLS_BINARY = "binary/test14.calls.bin";

    @Test
    void duration1s() {
        PodMetaData podInfo = getPodInfo();
        var pid = PodIdRestart.of(podInfo.pod().oldPodName());
        var ps = new PodSequence(pid, 1, Instant.EPOCH, Instant.EPOCH);

        var cf = new InternalCallFilter(DurationRange.ofSeconds(1, 4));
        var period = TimeRange.ofEpochMilli(1689255910000L, 1689257010000L);

        var task = new PodSequenceParser(period, podInfo, cf);
        var result = task.parseSequenceStream(ps, testZipDataStream(CALLS_BINARY));

        assertTrue(result.isSuccess());
        assertNotNull(result.calls());
        assertEquals(METHOD_1, calls(3, result).get(0).method());

    }

    private static List<CallRecord> calls(int expectedSize, CallSeqResult r) {
        var list = r.calls().toList();
        assertEquals(expectedSize, list.size());
        return list;
    }

    @Test
    void findAll() {
        PodMetaData podInfo = getPodInfo();

        var pid = PodIdRestart.of(podInfo.pod().oldPodName());
        var ps = new PodSequence(pid, 1, Instant.EPOCH, Instant.EPOCH);

        var cf = new InternalCallFilter(DurationRange.ofMillis(5, 140000));
        var period = TimeRange.ofEpochMilli(1689255910000L, 1689257010000L);

        var task = new PodSequenceParser(period, podInfo, cf);
        var result = task.parseSequenceStream(ps, testZipDataStream("binary/test14.calls.bin"));

        assertTrue(result.isSuccess());
        assertNotNull(result.calls());
        assertEquals(METHOD_1, calls(14, result).get(0).method());

    }

    @Test
    void findByParameters() {
        var calls = runSearch(query(""), 14);
        assertEquals(METHOD_1, calls.get(0).method());

        calls = runSearch(query("SocketProcessorBase"), 14); // InternalCallFilter doesn't work for method names
        assertEquals(METHOD_1, calls.get(0).method());

        calls = runSearch(query("+SocketProcessorBase"), 14); // InternalCallFilter doesn't work for method names
        assertEquals(METHOD_1, calls.get(0).method());

        calls = runSearch(query("+$param1=GET"), 5);
        assertEquals(METHOD_2, calls.get(0).method());

        calls = runSearch(query("-startDumper -$param1=GET"), 9);
        assertEquals(METHOD_1, calls.get(0).method());
    }

    private static InternalCallFilter query(String queryString) {
        return new InternalCallFilter(DurationRange.ofMillis(5, 140000), queryString);
    }

    private static List<CallRecord> runSearch(InternalCallFilter cf, int expectedCalls) {
        PodMetaData podInfo = getPodInfo();
        var pid = PodIdRestart.of(podInfo.pod().oldPodName());
        var ps = new PodSequence(pid, 1, Instant.EPOCH, Instant.EPOCH);

        cf = cf.enrich(Map.of("param1", 119));
        var period = TimeRange.ofEpochMilli(0L, 1889257010000L);

        var task = new PodSequenceParser(period, podInfo, cf);
        var result = task.parseSequenceStream(ps, testZipDataStream(CALLS_BINARY));
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertNotNull(result.calls());
        return calls(expectedCalls, result);
    }

    static PodMetaData getPodInfo() {
        var p = PodInfo.of("ns","service", "podName", Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);
        var podInfo = PodMetaData.empty(p);
        podInfo.enrichDb(List.of(), podDictionary());
        return podInfo;
    }

    public static final String METHOD_1 = "void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar]";
    public static final String METHOD_2 = "void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]";

    static List<DictionaryModel> podDictionary() {
        return List.of(sLiteral(7, "void com.netcracker.cdt.uiservice.UiServiceApplication.main(java.lang.String[]) (UiServiceApplication.java:58) [escui.jar!/BOOT-INF/classes]"),
                sLiteral(9, "void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar]"),
                sLiteral(19, "tag1"),
                sLiteral(41, "tag2"),
                sLiteral(84, "tag3"),
                sLiteral(119, "tag4"),
                sLiteral(168, "void java.io.FileOutputStream.write(byte[],int,int) (FileOutputStream.java:349)"),
                sLiteral(176, "java.lang.Class java.lang.Class.forName(java.lang.String) (Class.java:374)"),
                sLiteral(565, "void org.apache.tomcat.util.net.SocketProcessorBase.run() (SocketProcessorBase.java:41) [BOOT-INF/lib/tomcat-embed-core-9.0.74.jar]"),
                sLiteral(603, "boolean com.netcracker.mano.dim.services.impl.GraylogExporter$TargetStream.equals(java.lang.Object) (GraylogExporter.java:566) [BOOT-INF/lib/diagnostic-info-manager-22.1.1.0.0.jar]")
        );
    }

    static DictionaryModel sLiteral(int pos, String tag) {
        return new DictionaryModel(PodIdRestart.of("test_1234534"), pos, tag);
    }
}