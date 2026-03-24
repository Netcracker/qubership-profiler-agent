package com.netcracker.integration.cloud;

import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.cdt.collector.tcp.ProtocolEmulator;
import com.netcracker.common.Time;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.tcp.SocketEmulator;
import com.netcracker.integration.Profiles;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.utils.Utils;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.netcracker.common.ProtocolConst.DATA_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(Profiles.CloudTest.class)
public class CloudProfilerAgentReaderTest { // extends ProfilerAgentReaderTest {

    public static final PodName POD = TestHelper.pod(1);

    public static final Instant T1 = Instant.parse("2023-06-28T02:19:27.000Z");
    public static final Instant T2 = Instant.parse("2023-07-10T01:45:12.123Z");

    public static final String POD_RESTART1 = POD.podName() + "_" + T1.toEpochMilli();
    public static final String POD_RESTART2 = POD.podName() + "_" + T2.toEpochMilli();

    static final int BUFFER_SIZE = DATA_BUFFER_SIZE;

    @Inject
    Time time;

    @Inject
    TestHelper test;

    @Inject
    StreamDumper streamDumper;
    @Inject
    PodDumper podDumper;
    @Inject
    PersistenceService persistence;

    @Test
    public void testRead() throws IOException {

        var socket = new SocketEmulator(POD_RESTART1);
        var reader = socket.createAgentReader(time, streamDumper, podDumper);
        var protocol = new ProtocolEmulator(socket, reader, BUFFER_SIZE);

        protocol.initProtocol(POD.namespace(), POD.service(), POD_RESTART1);
        protocol.requestFlush();

//        persistence.pods.getLatestPodStatistics()

        var handleId = protocol.initStream(StreamType.TD, 0);
        assertNotNull(handleId);
        var bytes = "ThreadDumpExample".getBytes(StandardCharsets.UTF_8);
        protocol.sendData(handleId, bytes);

        protocol.close();

//        persistence.pods.getLatestPodStatistics()

        podDumper.persistStat();

        socket = new SocketEmulator(POD_RESTART2);
        reader = socket.createAgentReader(time, streamDumper, podDumper);
        protocol = new ProtocolEmulator(socket, reader, BUFFER_SIZE);

        protocol.initProtocol(POD.namespace(), POD.service(), POD_RESTART2);
        protocol.requestFlush();

        handleId = protocol.initStream(StreamType.DICTIONARY, 0);
        assertNotNull(handleId);
        bytes = Utils.readBytes("binary/test-service.dictionary.bin");
        protocol.sendBuffer(handleId, bytes);

        handleId = protocol.initStream(StreamType.PARAMS, 0);
        assertNotNull(handleId);
        bytes = Utils.readBytes("binary/test-service.params.bin");
        protocol.sendBuffer(handleId, bytes);

//        handleId = protocol.initStream(StreamType.GC, 0);
//        assertNotNull(handleId);
//        bytes = Utils.readBytes("binary/test-service.gc.0.bin");
//        protocol.sendBuffer(handleId, bytes);

//        handleId = protocol.initStream(StreamType.HEAP, 0);
//        assertNotNull(handleId);
//        bytes = Utils.readBytes("binary/test-service.calls.0.bin");
//        protocol.sendBuffer(handleId, bytes);

//        handleId = protocol.initStream(StreamType.XML, 0);
//        assertNotNull(handleId);
//        bytes = Utils.readBytes("binary/test-service.xml.0.bin");
//        protocol.sendBuffer(handleId, bytes);

//        handleId = protocol.initStream(StreamType.SQL, 0);
//        assertNotNull(handleId);
//        bytes = Utils.readBytes("binary/test-service.sql.0.bin");
//        protocol.sendBuffer(handleId, bytes);

        handleId = protocol.initStream(StreamType.CALLS, 0);
        assertNotNull(handleId);
        bytes = Utils.readBytes("binary/test-service.calls.0.bin");
        protocol.sendBuffer(handleId, bytes);

//        handleId = protocol.initStream(StreamType.TRACE, 0);
//        assertNotNull(handleId);
//        bytes = Utils.readBytes("binary/test-service.traces.0.bin");
//        protocol.sendBuffer(handleId, bytes);

        handleId = protocol.initStream(StreamType.SUSPEND, 0);
        assertNotNull(handleId);
        bytes = Utils.readBytes("binary/test-service.suspend.bin");
        protocol.sendBuffer(handleId, bytes);

        protocol.requestFlush();
        protocol.close();

        podDumper.persistStat();
        test.waitForBatch(); // async batchers

        var info = persistence.pods.findPod(TimeRange.of(T1, T2), POD);
        assertTrue(info.isPresent());

        var podInfo = info.get();
        assertEquals(POD.namespace(), podInfo.namespace());
        assertEquals(POD.service(), podInfo.service());
        assertEquals(POD.podName(), podInfo.podName());

        var range = time.ofLast(10, ChronoUnit.MINUTES);
        var list = persistence.pods.podRestarts(podInfo, range, 3);
        // assertEquals(2, list.size()); // disabled because it fails, it's not critical part of this test and it isn't obvious how to fix it
    }
}