package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.models.meta.DictionaryModel;
import com.netcracker.common.models.meta.ParamsModel;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class PodMetaDataTest {

    @Test
    void create() {
        var podName = "pod-name-aed4-orm_1675853926859";
        var pod = PodInfo.of("ns", "srv", podName);
        pod = pod.updateActive(Instant.parse("2023-08-01T01:02:03.456Z"));

        var meta = PodMetaData.empty(pod);
        assertTrue(meta.isValid());
        assertEquals(pod, meta.pod());
        assertEquals(pod.restartId(), meta.podId());
        assertEquals("ns", meta.namespace());
        assertEquals("srv", meta.service());
        assertEquals(podName, meta.oldPodName());
        assertEquals(podName, meta.toString());
        assertEquals(Instant.parse("2023-02-08T10:58:46.859Z"), meta.startTime());
        assertEquals(Instant.parse("2023-08-01T01:02:03.456Z"), meta.lastActive());
        assertEquals(Instant.parse("2023-02-08T10:58:46.859Z"), meta.podId().restartTime());
    }


    @Test
    void literals() {
        var pod = PodInfo.of("ns", "srv", "pod-name-aed4-orm_1675853926859");
        var meta = PodMetaData.empty(pod);
        assertTrue(meta.isValid());

        assertNull(meta.getLiteral(0));
        meta.putLiteral(0, "method1");

        assertNotNull(meta.getLiteral(0));
        assertEquals("method1", meta.getLiteral(0));

        assertNull(meta.getLiteral(1));
        meta.putLiteral(1,"method2");

        assertNotNull(meta.getLiteral(1));
        assertEquals("method2", meta.getLiteral(1));

    }

    @Test
    void proxy() {
        var p = pod("profiler", "ui-service", "ui-service-test",
                1685601161000L, 1689229961000L);
        var pm = PodMetaData.empty(p);
        assertEquals("profiler", pm.namespace());
        assertEquals("ui-service", pm.service());
        assertEquals(Instant.parse("2023-06-01T06:32:41Z"), pm.startTime());
        assertEquals(Instant.parse("2023-07-13T06:32:41Z"), pm.lastActive());
        assertEquals("ui-service-test_1685601161000", pm.oldPodName());
        assertEquals("ui-service-test_1685601161000", pm.toString());
        assertTrue(pm.isValid());
    }

    @Test
    void ensureParameters() {
        var pm = pod(1, "test");
        assertEquals(0, pm.tagsSize());
        assertEquals(0, pm.paramsSize());

        pm.putLiteral(1, "param1");
        pm.putLiteral(2, "param2");
        pm.putLiteral(3, "param3");

        pm.putParameter("param1", false, false, 11, "testMethod1");
        pm.putParameter("param2", false, true, 12, "testMethod2");
        assertEquals(2, pm.paramsSize());

        var res = pm.putParameter("param3", true, false, 1, "testMethod3");
        assertTrue(res);
        assertEquals(3, pm.paramsSize());

        res = pm.putParameter("param4", true, false, 1, "testMethod3");
        assertFalse(res);
        assertEquals(3, pm.paramsSize());

    }

    @Test
    void ensureLiterals() {
        var pm = pod(1, "test");
        assertEquals(0, pm.tagsSize());
        assertEquals(0, pm.paramsSize());

        assertNull(pm.getLiteral(0));

        pm.putLiteral(0, "keyword");

        assertNotNull(pm.getLiteral(0));
        assertEquals("keyword", pm.getLiteral(0));
        assertEquals(1, pm.tagsSize());

    }

    @Test
    void enrichDb() {
        var pm = pod(1, "test");
        assertEquals(0, pm.tagsSize());
        assertEquals(0, pm.paramsSize());

        pm.enrichDb(
                List.of(),
                List.of(sLiteral(1, "tag1"),
                        sLiteral(-1, "tag2"),
                        sLiteral(5, "tag3")));

        assertEquals(3, pm.tagsSize());
        assertEquals(0, pm.paramsSize());

        pm.enrichDb(
                List.of(sParam("param1", false, false, 10, "method1"),
                        sParam("param2", false, true, 20, "method2"),
                        sParam("param3", true, false, 30, "method3")),
                List.of(sLiteral(10, "tag10"),
                        sLiteral(11, "tag11"),
                        sLiteral(12, "tag12"),
                        sLiteral(13, "param1"),
                        sLiteral(14, "param2"),
                        sLiteral(15, "param3")
                        ));
        assertEquals(9, pm.tagsSize());
        assertEquals(3, pm.paramsSize());

    }

    private static PodMetaData pod(int id, String podName) {
        return PodMetaData.empty(pod(podName));
    }

    private static PodInfo pod(String podName) {
        return pod("a", "b", podName, 0, 0);
    }

    private static PodInfo pod(String ns, String service, String podName, long start, long active) {
        return PodInfo.of(ns, service, podName+"_"+start,
                Instant.ofEpochMilli(start), Instant.ofEpochMilli(start), Instant.ofEpochMilli(active));
    }

    private ParamsModel sParam(String param, boolean idx, boolean list, int order, String signature) {
        return new ParamsModel(PodIdRestart.of("test_123"), param, idx, list, order, signature);
    }

    private DictionaryModel sLiteral(int pos, String tag) {
        return new DictionaryModel(PodIdRestart.of("test_123"), pos, tag);
    }
}