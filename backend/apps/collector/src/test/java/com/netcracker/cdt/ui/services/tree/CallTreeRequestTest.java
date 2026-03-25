package com.netcracker.cdt.ui.services.tree;

import com.netcracker.cdt.ui.services.tree.CallTreeRequest;
import com.netcracker.utils.UnitTest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class CallTreeRequestTest {

    @Test
    void workflow() {
        var trace = "1_1_903860_0_0_0";
        var pods = List.of("esc-collector-service-55c6cccf75-dlg5x_1689598329648");

        var params = params(Map.of(
                "id", "1",
                "params-trim-size", 1500,
                "s", 1689599398871L,
                "e", 1689599403875L,
                "clientUTC", System.currentTimeMillis(),
                "callback", "treedata",
                "i", trace,
                "f[_1]", pods
        ));

        var req = CallTreeRequest.from(Instant.now(), false, params);
        assertEquals(2, req.args().size());
        assertEquals(Map.of("i",  List.of(trace), "f[_1]", pods), req.args());

        req = CallTreeRequest.from(Instant.now(), true, params);
        assertEquals(3, req.args().size());
        assertEquals(Map.of("i",  List.of(trace), "ro", "1", "f[_1]", pods), req.args());
    }

    HeadersMultiMap params(Map<String, Object> m) {
        var r = new HeadersMultiMap();
        for (var e: m.entrySet()) {
            if (e.getValue() instanceof String) {
                r.add(e.getKey(), (String) e.getValue());
            } else if (e.getValue() instanceof Iterable) {
                r.add(e.getKey(), (Iterable<?>) e.getValue());
            }
        }
        return r;
    }
}