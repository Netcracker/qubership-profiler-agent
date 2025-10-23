package com.netcracker.cdt.ui.models;

import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class PodsIndexTest {

    @Test
    void workflow() {
        var idx = PodsIndex.create();
        assertEquals(0, idx.size());

        var pod1 = PodInfo.of("ns", "srv", "pod-name-aed4-orm_1675853926859");
        var meta = idx.ensure(pod1);
        assertEquals(1, idx.size());

        assertTrue(meta.isValid());
        assertEquals(pod1, meta.pod());
        assertEquals("pod-name-aed4-orm_1675853926859", meta.oldPodName());

        var metaCopy = idx.ensure(pod1);
        assertEquals(1, idx.size());
        assertEquals(meta, metaCopy);


        var pod2 = PodInfo.of("ns", "srv", "pod-name-ere-orm_1675853926859");
        var meta2 = idx.ensure(pod2);
        assertEquals(2, idx.size());

        assertTrue(meta2.isValid());
        assertEquals(pod2, meta2.pod());
        assertEquals("pod-name-ere-orm_1675853926859", meta2.oldPodName());


    }

}