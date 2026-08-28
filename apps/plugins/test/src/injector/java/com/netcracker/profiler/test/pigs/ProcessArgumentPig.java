package com.netcracker.profiler.test.pigs;

import java.util.HashMap;
import java.util.Map;

public class ProcessArgumentPig {
    private int processInt$profiler(int x) {
        return x + 7;
    }

    private Object processObject$profiler(Object x) {
        return "42";
    }

    private Map processMap$profiler(Map x) {
        Map m = new HashMap(x);
        m.put("test", "after process");
        return m;
    }
}
