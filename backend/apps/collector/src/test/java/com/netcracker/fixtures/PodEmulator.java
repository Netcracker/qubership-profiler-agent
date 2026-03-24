package com.netcracker.fixtures;

import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.cdt.collector.tcp.ProtocolEmulator;
import com.netcracker.common.Time;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.pod.stat.BlobSize;
import com.netcracker.common.models.pod.stat.PodRestartStat;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.fixtures.data.BinaryFile;
import com.netcracker.fixtures.data.PodBinaryData;
import com.netcracker.fixtures.tcp.SocketEmulator;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.persistence.utils.ByteBufferInputStream;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.netcracker.common.ProtocolConst.DATA_BUFFER_SIZE;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

public class PodEmulator {
    private static final int BUFFER_SIZE = DATA_BUFFER_SIZE;
    private static DateTimeFormatter DUMP_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss").withZone(ZoneId.of("UTC"));

    public final PodAsserts asserts;
    public final PodPersistence retrieve;

    private final TestHelper helper;
    private final Time time;
    private final StreamDumper streamDumper;
    private final PodDumper podDumper;
    private final PersistenceService persistence;
    private final PodName pod;

    private PodIdRestart podId;
    private Instant activeSince, restartTime;
    private Set<Instant> restartTimes = new TreeSet<>();
    private ProtocolEmulator tcp;
    private PodBinaryData emulatedData;
    private Map<StreamType, Integer> latestSeq = new HashMap<>();

    public PodEmulator(TestHelper helper, Time time,
                       StreamDumper streamDumper, PodDumper podDumper, PersistenceService persistence,
                       PodName pod) {
        this.helper = helper;
        this.time = time;
        this.streamDumper = streamDumper;
        this.podDumper = podDumper;
        this.persistence = persistence;
        this.pod = pod;
        this.asserts = new PodAsserts();
        this.retrieve = new PodPersistence();
    }

    public void start(Instant t, PodBinaryData data) throws Exception {
        try (var ignored = helper.withTime(t)) {
            assertNull(activeSince, "pod was already started");
            podId = PodIdRestart.of(pod, t);
            tcpStart();
            activeSince = t;
            restartTime = t;
            restartTimes.add(t);
            initPod(data);
        }
    }

    public void restart(Instant t, PodBinaryData data) throws Exception {
        try (var ignored = helper.withTime(t)) {
            assertNotNull(activeSince, "pod has not started yet");
            assertFalse(restartTimes.contains(t), "pod was already restarted with with timestamp: " + t);
            tcpClose();
            podId = PodIdRestart.of(pod, t);
            tcpStart();
            restartTime = t;
            restartTimes.add(t);
            initPod(data);
        }
    }

    public PodName getPod() {
        return pod;
    }

    public PodIdRestart getPodId() {
        return podId;
    }

    public Instant getRestartTime() {
        return restartTime;
    }

    public void sendStream(StreamType type) throws IOException {
        var seqId = 1 + latestSeq.getOrDefault(type, -1);
        var handleId = tcp.initStream(type, seqId);
        assertNotNull(handleId);
        var bytes = emulatedData.getBytes(type);
        tcp.sendBuffer(handleId, bytes);
        latestSeq.put(type, seqId);
    }

    public void sendCalls(Instant t, File seqFile) {

    }

    public byte[] downloadHeapDump(int seqId) {
        return downloadDump(StreamType.HEAP, seqId);
    }

    public void persistStat() {
        podDumper.persistStat();
        helper.waitForBatch();
    }

    public void finish() {
        try {
            tcp.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void tcpStart() throws IOException {
        assertNotNull(podId);
        var socket = new SocketEmulator(podId.podName());
        var reader = socket.createAgentReader(time, streamDumper, podDumper);
        tcp = new ProtocolEmulator(socket, reader, BUFFER_SIZE);
    }

    void tcpClose() {
        if (tcp == null) return;
        tcp.socket().done();
        tcp = null;
        podId = null;
    }

    void initPod(PodBinaryData data) throws IOException {
        assertNotNull(tcp);
        assertNotNull(podId);
        assertFalse(podId.isEmpty());
        emulatedData = data;
        assertNotNull(emulatedData);

        // init conn, register
        tcp.initProtocol(pod.namespace(), pod.service(), podId.podId());
        tcp.requestFlush();

        if (data.has(StreamType.DICTIONARY)) {
            sendStream(StreamType.DICTIONARY);
        }
        if (data.has(StreamType.PARAMS)) {
            sendStream(StreamType.PARAMS);
        }

        // assert
        persistStat();
        asserts.restartTime(restartTime);
        asserts.activeSince(activeSince);
        asserts.lastActive(time.now());

        var restarts = asserts.podRestarts(restartTimes.size());
        var actualRestartTimes = restarts.stream().map(PodInfo::restartTime).collect(Collectors.toSet());
        assertEquals(restartTimes, actualRestartTimes);
    }

    byte[] downloadDump(StreamType dumpType, int seqId) {
        // TODO replace to /cdt/v2/
        String dumpName = dumpType.getName().toLowerCase();
        String handle = podId.oldPodName() + "_" + dumpName + "_" + seqId;
        var resp = given()
                .queryParam("handle", handle)
                .when().get("/esc/downloadHeapDump")
                .then()
                .statusCode(200)
                .extract().body().asByteArray();
        assertNotNull(resp);
        assertNotEquals(0, resp.length);
        return resp;
    }

    public class PodAsserts {

        static void assertSameTime(Instant expected, Instant actual, String message) {
            assertTrue(Math.abs(expected.toEpochMilli() - actual.toEpochMilli()) < 2000, // delta ~2s
                    String.format("%s. Expected %s, got %s", message, expected, actual));
        }

        public void lastActive(Instant expected) {
            var podInfo = retrieve.podData();
            assertSameTime(expected, podInfo.lastActive(), "invalid lastActive");
        }
        public void activeSince(Instant expected) {
            var podInfo = retrieve.podData();
            assertSameTime(expected, podInfo.activeSince(), "invalid activeSince");
        }
        public void restartTime(Instant expected) {
            var podInfo = retrieve.podData();
            assertSameTime(expected, podInfo.restartTime(), "invalid restartTime");
        }

        public List<PodInfo> podRestarts(int expected) {
            return retrieve.podRestarts(expected);
        }

        public List<PodRestartStat> podStat(int expected) {
            var list = retrieve.podStat();
            assertEquals(expected, list.size());
            return list;
        }

        public PodRestartStat latestStat() {
            var stats = podStat(1);
            return stats.get(0);
        }

        public void latestStat(StreamType type, BlobSize expected) {
            var stat = latestStat();
            assertEquals(restartTime, stat.pod().restartTime());
            assertEquals(expected, stat.accumulated().map().get(type));
        }

    }

    public static Map<String, byte[]> readZip(byte[] zipFile) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(zipFile);
        Map<String, byte[]> res = new HashMap<>();
        try (ZipInputStream zin = new ZipInputStream(new ByteBufferInputStream(buf))) {
            ZipEntry ze;
            while (((ze = zin.getNextEntry()) != null)) {
                boolean contentsZipped = ze.getName().toLowerCase().endsWith(".gz");
                var data = IOUtils.toByteArray(getInputStream(zin, contentsZipped));
                res.put(ze.getName(), data);
            }
        }
        return res;
    }

    public static InputStream getInputStream(ZipInputStream zin, boolean unzip) throws IOException {
        InputStream result;
        if (unzip) {
            result = new GZIPInputStream(zin);
        } else {
            result = zin;
        }
        return new BufferedInputStream(result, Short.MAX_VALUE);
    }

    public class PodPersistence {

        public PodInfo podData() {
            helper.waitForBatch();
            var wholeTime = time.tillNow();
            var info = persistence.pods.findPod(wholeTime, pod);
            assertTrue(info.isPresent(), "could not find pod " + pod.podName());
            assertEquals(pod.namespace(), info.get().namespace());
            assertEquals(pod.service(), info.get().service());
            assertEquals(pod.podName(), info.get().podName());
            assertTrue(info.get().podId().contains(pod.id()));
            return info.get();
        }

        public List<PodInfo> podRestarts(int expected) {
            helper.waitForBatch();
            var podInfo = podData();
            var range = time.ofLast(2, ChronoUnit.DAYS);
            var list = persistence.pods.podRestarts(podInfo, range, expected + 1);

            assertNotEquals(0, list.size());
            assertEquals(expected, list.size());

            for (var p: list) {
                assertEquals(pod.namespace(), p.namespace());
                assertEquals(pod.service(), p.service());
                assertEquals(pod.podName(), p.podName());
            }
            return list;
        }

        public List<PodRestartStat> podStat() {
            helper.waitForBatch();
            var list = persistence.pods.getLatestPodsStatistics(List.of(podId), time.now());
            return list;
        }

        public StreamRegistry registry(StreamType type, int seqId) {
            helper.waitForBatch();
            var registry = persistence.streams.getStreamRegistryById(podId, type, seqId);
            assertTrue(registry.isPresent());
            return registry.get();
        }

        public ByteBuffer persistedDump(StreamType type, int seqId) throws IOException {
            var sr = registry(type, seqId);
            var is = persistence.streams.getStream(sr);
            var bytes = IOUtils.toByteArray(is);
            assertNotEquals(0, bytes.length);
            return ByteBuffer.wrap(bytes);
        }

    }

}
