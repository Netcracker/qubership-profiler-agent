package com.netcracker.common.models.dict;

import static com.netcracker.cdt.ui.services.calls.models.Utils.*;
import com.netcracker.utils.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class ParametersTest {

    @Test
    void isSystem() {
        assertFalse(params(pv("test", "v1", "v2")).isSystem());
        assertFalse(params(epv("test")).isSystem());
        assertTrue(params(epv("calls.idle")).isSystem());
        assertTrue(params(epv( "async.absorbed")).isSystem());
        assertFalse(params(epv("web.url")).isSystem());
        assertTrue(params(pv("web.url", "/probes/live")).isSystem());
        assertTrue(params(pv("web.url", "host:port/probes/live")).isSystem());
        assertFalse(params(pv("web.url", "host:port/probes/A/live")).isSystem());
    }

}