package com.netcracker.common.models.streams;

import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.models.pod.PodName;
import com.netcracker.common.models.pod.streams.PodSequence;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class PodSequenceTest {

    public static final String A = "service-a";
    public static final String B = "service-b";
    public static final String C = "service-c";

    @Test
    void sort() {
        var l = new ArrayList<PodSequence>();
        l.add(ps(A, 3));
        l.add(ps(A, 2));
        l.add(ps(B, 1));
        l.add(ps(B, 2));
        l.add(ps(B, 3));
        l.add(ps(A, 4));
        l.add(ps(A, 1));
        l.add(ps(C, 1));
        l.add(ps(C, 1));
        assertEquals(9, l.size());

        var t = new TreeSet<PodSequence>();
        t.add(ps(A, 3));
        t.add(ps(A, 2));
        t.add(ps(B, 1));
        t.add(ps(B, 2));
        t.add(ps(B, 3));
        t.add(ps(A, 4));
        t.add(ps(A, 1));
        t.add(ps(C, 1));
        t.add(ps(C, 1)); // duplicate
        assertEquals(8, t.size());

        var a = t.stream().toList();
        assertEquals(ps(A, 1), a.get(0));
        assertEquals(ps(A, 2), a.get(1));
        assertEquals(ps(A, 3), a.get(2));
        assertEquals(ps(A, 4), a.get(3));
        assertEquals(ps(B, 1), a.get(4));
        assertEquals(ps(B, 2), a.get(5));
        assertEquals(ps(B, 3), a.get(6));
        assertEquals(ps(C, 1), a.get(7));
    }

    @Test
    void expandCreatedModified() {
        PodSequence origin = ps(A, 1, 10, 20);

        // [A:10   {B:19   A:20]   B:30}
        assertTrue(ps(A, 1, 10, 30).same(
                origin.
                        expandCreatedModified(ps(A, 1, 19, 30))
        ));
        // [A:10   {B:11   B:15}   A:20]
        assertTrue(origin.same(
                origin.
                        expandCreatedModified(ps(A, 1, 11, 15))
        ));
        // {B:5   [A:10   A:20]   B:60}
        assertTrue(ps(A, 1, 5, 60).same(
                origin.
                        expandCreatedModified(ps(A, 1, 5, 60))
        ));

        // check no changes for alien seq:
        assertTrue(origin.same(
                origin.expandCreatedModified(ps(B, 1, 20, 30))
        ));
        assertTrue(origin.same(
                origin.expandCreatedModified(ps(A, 2, 20, 30))
        ));
    }

    private static PodSequence ps(String service, int sequenceId) {
        return ps(service, sequenceId, 0, 0);
    }

    private static PodSequence ps(String service, int sequenceId, long created, long modified) {
        var podId = PodIdRestart.of(service+"-pod_12345456");
        return new PodSequence(podId, sequenceId,
                Instant.ofEpochMilli(created),
                Instant.ofEpochMilli(modified)
        );
    }

    private static PodName pod(String podName) {
        return new PodName(A, B, podName, "podId");
    }

}