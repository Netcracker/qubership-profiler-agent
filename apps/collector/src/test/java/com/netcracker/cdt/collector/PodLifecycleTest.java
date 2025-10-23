package com.netcracker.cdt.collector;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.common.models.pod.stat.BlobSize;
import com.netcracker.fixtures.TestHelper;
import com.netcracker.fixtures.data.PodBinaryData;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class PodLifecycleTest {
    public static final PodName POD = TestHelper.pod(1);
    public static final PodBinaryData DATA = PodBinaryData.TEST_SERVICE;

    public static final Instant T1 = Instant.parse("2023-06-28T02:19:27.000Z");
    public static final Instant T2 = Instant.parse("2023-06-28T02:30:00.123Z");
    public static final Instant T3 = Instant.parse("2023-06-28T02:55:00Z");

    @Inject
    TestHelper test;

    public void testInit() throws Exception {
        var emulator = test.startPod(T1, POD, DATA);

        try (var ignored = test.withTime(T1)) {
            emulator.asserts.latestStat(StreamType.DICTIONARY, BlobSize.of(331255, 331255));
            emulator.asserts.latestStat(StreamType.PARAMS, BlobSize.of(3704, 3704));
            var stat = emulator.asserts.latestStat().accumulated();
            assertFalse(stat.hasTD(), "should not be stat before");
        }

        try (var ignored = test.withTime(T3)) {
            emulator.restart(T3, DATA);

            emulator.asserts.latestStat(StreamType.DICTIONARY, BlobSize.of(331255, 331255));
            emulator.asserts.latestStat(StreamType.PARAMS, BlobSize.of(3704, 3704));
        }
    }
}
