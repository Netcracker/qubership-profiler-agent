package com.netcracker.cdt.ui.services.tree.context;

import com.netcracker.common.models.meta.Value;
import com.netcracker.cdt.ui.services.tree.CallTreeRequest;
import com.netcracker.cdt.ui.services.tree.data.ProfiledTree;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.data.PodBinaryData;
import com.netcracker.profiler.model.CallRowId;
import com.netcracker.profiler.model.TreeRowId;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.netcracker.utils.Utils.setOf;
import static org.junit.jupiter.api.Assertions.*;

public abstract class TraceReaderTest {
    public static final PodName POD = TestHelper.pod(1);
    public static final Instant T1 = Instant.parse("2023-06-28T02:19:27.000Z");
    public static final PodBinaryData DATA = PodBinaryData.U5MIN_SERVICE;

    @Inject
    TestHelper test;

    @Inject
    TreeDataLoader treeDataLoader;

    @Test
    void read() throws Exception {
        try (var ignored = test.withTime(T1)) {

            var emulator = test.startPod(T1, POD, DATA);
            var podId = emulator.getPodId().oldPodName();

            emulator.sendStream(StreamType.TRACE);
//        emulator.sendStream(StreamType.CALLS);
            emulator.sendStream(StreamType.SQL);
            emulator.sendStream(StreamType.XML);
            emulator.persistStat();
            emulator.finish();


            var rowId = row(8, 0);
            var tree = readTree(podId, rowId);
            assertNotNull(tree);
            assertEquals(rowId, tree.getRowid());

            assertEquals(0, tree.ganttInfos.size());
            assertEquals(0, tree.getClobValues().uniqToLoad().size());
            assertEquals(setOf(0, 9, 18, 20, 21, 24, 33, 35, 148), tree.getDict().getIds());
            assertEquals(block8Methods(), tree.getDict().getTagMap());
//        assertEquals(1, tree.getRoot().id);

            rowId = row(8281, 0);
            tree = readTree(podId, rowId);
            assertNotNull(tree);
            assertEquals(rowId, tree.getRowid());
            var clobs = new ArrayList<>(tree.getClobValues().getClobs());
            assertEquals(2, clobs.size());
            assertEquals(0, tree.getClobValues().uniqToLoad().size());
            assertEquals(List.of(Value.clob(podId, StreamType.SQL, 1, 445), Value.clob(podId, StreamType.XML, 1, 0)), clobs);
            assertEquals("select * from active_pods where active_during_hour = ?", clobs.get(0).get());
            assertEquals("TIMESTAMP: active_during_hour: 2023-08-04T16:00:00Z\n", clobs.get(1).get());
            assertEquals(setOf(0, 5, 66, 596), tree.getDict().getIds());
            assertEquals(block8281Methods(), tree.getDict().getTagMap());

            rowId = row(13440, 0);
            tree = readTree(podId, rowId);
            assertNotNull(tree);
            assertEquals(rowId, tree.getRowid());
        }

    }

    private Map<Integer, String> block8Methods() {
        return Map.of(0, "call.info",
                9, "void com.netcracker.profiler.agent.Profiler.startDumper() (Profiler.java:20) [profiler-runtime.jar]",
                18, "common.started",
                20, "node.name",
                21, "java.thread",
                24, "time.cpu",
                33, "java.lang.String com.netcracker.profiler.ServerNameResolver.parsePODName() (ServerNameResolver.java:23) [ncdiag/lib/runtime.jar]",
                35, "void com.netcracker.profiler.ServerNameResolver.<clinit>() (ServerNameResolver.java:14) [ncdiag/lib/runtime.jar]",
                148, "void com.netcracker.profiler.formatters.title.TitleFormatterFacade.initFormatters() (TitleFormatterFacade.java:22) [ncdiag/lib/runtime.jar]");
    }
    private Map<Integer, String> block8281Methods() {
        return Map.of(0, "call.info",
                5, "sql",
                66, "binds",
                596, "void com.datastax.oss.driver.internal.core.cql.CqlRequestHandler.sendRequest(com.datastax.oss.driver.api.core.cql.Statement,com.datastax.oss.driver.api.core.metadata.Node,java.util.Queue,int,int,boolean) (CqlRequestHandler.java:245) [BOOT-INF/lib/java-driver-core-4.14.1.jar]");
    }

    private TreeRowId row(int offset, int record) {
        var s = String.format("1_1_%d_%d_0_0", offset, record);
        return new TreeRowId(1, s, 1, offset, 0);
    }

    private ProfiledTree readTree(String podId, TreeRowId id) {
        var params = Map.<String, Object>of("f[_1]", List.of(podId));
        var req = new CallTreeRequest(1, false,
                10000, 150, System.currentTimeMillis(),
                "treedata", "", "", "",
                params,  List.of(CallRowId.parse(id.fullRowId, params)),
                1689599398871L, 1689599403875L);
        var reader = new TraceRequestReader(treeDataLoader, req);
        return reader.read();
    }
}