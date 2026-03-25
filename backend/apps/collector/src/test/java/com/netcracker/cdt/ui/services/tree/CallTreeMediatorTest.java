package com.netcracker.cdt.ui.services.tree;

import com.netcracker.cdt.ui.services.tree.context.TreeDataLoader;
import com.netcracker.cdt.ui.services.tree.context.TraceRequestReader;
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
import java.util.List;
import java.util.Map;

import static com.netcracker.utils.Utils.setOf;
import static org.junit.jupiter.api.Assertions.*;

public abstract class CallTreeMediatorTest {
    public static final PodName POD = TestHelper.pod(1);
    public static final Instant T1 = Instant.parse("2023-06-28T02:19:27.000Z");
    public static final PodBinaryData DATA = PodBinaryData.U5MIN_SERVICE;

    @Inject
    TestHelper test;

    @Inject
    TreeDataLoader treeDataLoader;

    @Test
    void render() throws Exception {
        try (var ignored = test.withTime(T1)) {

            var emulator = test.startPod(T1, POD, DATA);
            var podId = emulator.getPodId().oldPodName();

            emulator.sendStream(StreamType.TRACE);
//            emulator.sendStream(StreamType.CALLS);
            emulator.sendStream(StreamType.SQL);
            emulator.sendStream(StreamType.XML);
            emulator.persistStat();
            emulator.finish();

            var rowId = row(8281, 0);
            var tree = readTree(podId, rowId);
            assertNotNull(tree);
            assertEquals(rowId, tree.getRowid());
            assertEquals(2, tree.getClobValues().getClobs().size());
            assertEquals(setOf(0, 5, 66, 596), tree.getDict().getIds());

            var mediator = new CallTreeMediator(getCallTreeRequest(podId, rowId));
            var res = mediator.render(tree);
        }

//        assertEquals(Utils.readString("tree/tree_8281_0.js"), res); // TODO random sorting of nodes in output

    }

    private TreeRowId row(int offset, int record) {
        var s = String.format("1_1_%d_%d_0_0", offset, record);
        return new TreeRowId(1, s, 1, offset, 0);
    }

    private ProfiledTree readTree(String podId, TreeRowId id) {
        var req = getCallTreeRequest(podId, id);
        var reader = new TraceRequestReader(treeDataLoader, req);
        return reader.read();
    }

    private CallTreeRequest getCallTreeRequest(String podId, TreeRowId id) {
        var params = Map.<String, Object>of("f[_1]", List.of(podId));
        var req = new CallTreeRequest(1, false,
                10000, 150, System.currentTimeMillis(),
                "treedata", "", "", "",
                params,  List.of(CallRowId.parse(id.fullRowId, params)),
                1689599398871L, 1689599403875L);
        return req;
    }
}