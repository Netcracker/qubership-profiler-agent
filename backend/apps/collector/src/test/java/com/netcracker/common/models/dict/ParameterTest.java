package com.netcracker.common.models.dict;

import com.netcracker.common.models.meta.dict.Parameter;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class ParameterTest {

    @Test
    void isIdle() {
        assertTrue(param("calls.idle").isIdle());
        assertFalse(param("calls.iddle").isIdle());
        assertTrue(param("async.absorbed").isIdle());
    }

    @Test
    void isInvalid() {
        assertTrue(param("").isInvalid());
        assertFalse(param("calls.idle").isInvalid());
    }

    @Test
    void equal() {
        assertEquals(param("test", 0), param("test", 0));
        assertEquals(param("test", 0), param("test", 1));
        assertNotEquals(param("test", 0), param("testA", 1));
        assertEquals("[name]", param("name", 1).toString());
    }

    private static Parameter param(String name) {
        return param(name, 0);
    }

    private static Parameter param(String name, int order) {
        return Parameter.of(name, false, false, order, name);
    }

    @Test
    void is() {
    }

}