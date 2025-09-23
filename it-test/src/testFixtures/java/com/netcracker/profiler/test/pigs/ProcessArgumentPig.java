package com.netcracker.profiler.test.pigs;

import java.util.Map;

public class ProcessArgumentPig {
    public static class Observer {
        public static void intArg(int x) {
        }

        public static void objectArg(Object x) {
        }
    }

    public int intArg(int x) {
        Observer.intArg(x);
        return x + 3;
    }

    public void objectArg(Object x) {
        Observer.objectArg(x);
    }

    public String mapArg(Map x) {
        Observer.objectArg(x.get("test"));
        return (String) x.get("x");
    }
}
