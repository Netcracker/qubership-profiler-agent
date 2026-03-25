package com.netcracker.fixtures;

import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.Time;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.data.PodBinaryData;
import com.netcracker.persistence.PersistenceService;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Singleton
public class TestHelper {

    @Inject
    Time time;

    @Inject
    StreamDumper streamDumper;
    @Inject
    PodDumper podDumper;
    @Inject
    PersistenceService persistence;

    @PostConstruct
    void init() {
        time.setTime(Instant.now());
    }

    public void setTime(Instant curTime) { // TODO: must NOT be used in tests directly, use `withTime` instead
        time.setTime(curTime);
    }

    public static PodName pod(int id) {
        var testName = retrieveTestName(3);
        var pod = testName + "-" + testName + "-" + id;
        return PodName.of(testName, testName, pod);
    }

    public static PodName pod(String namespacePrefix, String servicePrefix, int id) {
        var testName = retrieveTestName(3);
        assertThat(namespacePrefix,
                not(anyOf(containsString("."), containsString("_"))));
        assertThat(servicePrefix,
                not(anyOf(containsString("."), containsString("_"))));
        var ns = namespacePrefix + testName;
        var svc = servicePrefix + testName;
        var pod = svc + "-" + testName + "-" + id;
        return PodName.of(ns, svc, pod);
    }

    private static String retrieveTestName(int offset) {
        var stack = Thread.currentThread().getStackTrace(); // slow, but ok for test
        assertThat(stack.length, greaterThan(offset));
        var fileName = stack[offset].getFileName();
        return fileName.replace(".java", "");
    }

    // emulate specific "current" time for tests, should be used with `try...resources` block
    public AutoCloseable withTime(Instant t) {
        var was = time.now();
        time.setTime(t);
        assertThat(t, equalTo(time.now()));

        return () -> {
            time.setTime(was);
            assertThat(was, equalTo(time.now()));
        };
    }

    public PodEmulator podEmulator(PodName podName) {
        return new PodEmulator(this, time, streamDumper, podDumper, persistence, podName);
    }

    public PodEmulator startPod(Instant t, PodName podName, PodBinaryData data) throws Exception {
        var emulator = podEmulator(podName);
        emulator.start(t, data);
        return emulator;
    }

    public void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitForBatch() {
        sleep(10); // wait for async batcher for open-search itests
    }

    public void flushIndexes() {
        persistence.batch.flushIndexes(); // necessary for OpenSearch
    }
}
