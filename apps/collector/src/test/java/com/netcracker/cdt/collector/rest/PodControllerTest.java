package com.netcracker.cdt.collector.rest;

import com.netcracker.common.models.pod.PodName;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.data.PodBinaryData;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public abstract class PodControllerTest {
    public static final PodName POD = TestHelper.pod(1);
    public static final Instant T1 = Instant.parse("2023-06-28T02:19:27.000Z");
    public static final Instant T2 = Instant.parse("2023-07-10T01:45:12.123Z");

    @Inject
    TestHelper test;

    @Test
    public void testInit() throws Exception {
        var emulator = test.startPod(T1, POD, PodBinaryData.TEST_SERVICE);

    }

}
