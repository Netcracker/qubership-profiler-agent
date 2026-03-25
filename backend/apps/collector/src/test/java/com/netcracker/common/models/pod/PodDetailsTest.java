package com.netcracker.common.models.pod;

import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class PodDetailsTest {

    @Test
    void validPodNameParsing() {
        PodIdRestart.Parsed parsed = PodIdRestart.Parsed.fromOriginal("esc-collector-service-55c6cccf75-dlg5x_1687665091565");
        assertTrue(parsed.isValid());
        assertEquals("esc-collector-service", parsed.service());
        assertEquals("esc-collector-service-55c6cccf75-dlg5x", parsed.pod());
        assertEquals(Instant.parse("2023-06-25T03:51:31.565Z"), parsed.start());

        parsed = PodIdRestart.Parsed.fromOriginal("esc-test-service-58dfcb97-n4f7w_1675853926859");
        assertTrue(parsed.isValid());
        assertEquals("esc-test-service", parsed.service());
        assertEquals("esc-test-service-58dfcb97-n4f7w", parsed.pod());
        assertEquals(Instant.parse("2023-02-08T10:58:46.859Z"), parsed.start());

        parsed = PodIdRestart.Parsed.fromOriginal("esc-ui-service-5849b87b9b-85nng_1683814072058");
        assertTrue(parsed.isValid());
        assertEquals("esc-ui-service", parsed.service());
        assertEquals("esc-ui-service-5849b87b9b-85nng", parsed.pod());
        assertEquals(Instant.parse("2023-05-11T14:07:52.058Z"), parsed.start());

    }

    @Test
    void validTimestampParsing() {
        Instant t = PodIdRestart.retrievePodTimestamp("esc-collector-service-55c6cccf75-dlg5x_1687665091565");
        assertEquals(Instant.parse("2023-06-25T03:51:31.565Z"), t);

        t = PodIdRestart.retrievePodTimestamp("esc-test-service-58dfcb97-n4f7w_1675853926859");
        assertEquals(Instant.parse("2023-02-08T10:58:46.859Z"), t);

        t = PodIdRestart.retrievePodTimestamp("esc-ui-service-5849b87b9b-85nng_1683814072058");
        assertEquals(Instant.parse("2023-05-11T14:07:52.058Z"), t);

    }

    @Test
    void invalidPodNameParsing() {
        PodIdRestart.Parsed parsed = PodIdRestart.Parsed.fromOriginal("esc-collector-service-55c6cccf75-dlg5x_1687665091565a");
        assertFalse(parsed.isValid());
        assertEquals("", parsed.service());
        assertEquals("", parsed.pod());
        assertEquals(Instant.EPOCH, parsed.start());
    }
}