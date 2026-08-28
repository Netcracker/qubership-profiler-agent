package com.netcracker.profiler.test.pigs;

public class ExecuteMethodPig {
    public static class Observer {
        public static void staticMethod1(int a, byte b, short c, long d, double e, float f,
                                         int[] h, byte[] i, Integer g, String[] k) {
        }

        public static void staticMethod2(int a, byte b, short c, long d, double e, float f,
                                         int[] h, byte[] i, Integer g, String[] k) {
        }

        public static void executeInstead1(String a, long b) {
        }

        public static void executeInstead2(String a, long b) {
        }

        public static void throwException(String a, Throwable t, long b, Throwable caught) {
        }

        public static void throwExceptionOnly(String a, Throwable t, long b, Throwable caught) {
        }

        public static void throwExceptionJustThrowable(Throwable caught) {
        }
    }

    public static int staticExecuteBefore(int a, byte b, short c, long d, double e, float f,
                                          int[] h, byte[] i, Integer g, String[] k) {
        int x = a + b;
        if (System.currentTimeMillis() > x || System.currentTimeMillis() > 0)
            Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
        return 0;
    }

    public static int staticExecuteAfter(int a, byte b, short c, long d, double e, float f,
                                         int[] h, byte[] i, Integer g, String[] k) {
        int x = a + b;
        if (System.currentTimeMillis() > x || System.currentTimeMillis() > 0)
            Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
        return 0;
    }

    public Integer staticExecuteAfterWithResult(int a, byte b, short c, long d, double e, float f,
                                         int[] h, byte[] i, Integer g, String[] k) {
        try {
            int x = a + b;
            try {
                if (System.currentTimeMillis() > x || System.currentTimeMillis() > 0)
                    Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
            } catch (Throwable t) {
                t.toString();
                return g;
            }
            return g;
        } catch (NumberFormatException ex) {
            // ignore
        }
        return g;
    }

    public long executeInstead(String a, long b) throws Throwable {
        try {
            String x = a + b;
            if (System.currentTimeMillis() > x.hashCode() || System.currentTimeMillis() > 0)
                Observer.executeInstead1(a, b);
            return 0;
        } catch (Throwable t) {
            throw t;
        }
    }

    public long executeNewInstead(String a, long b) throws Throwable {
        try {
            String x = a + b;
            if (System.currentTimeMillis() > x.hashCode() || System.currentTimeMillis() > 0)
                Observer.executeInstead2(a, b);
            return 0;
        } catch (Throwable t) {
            throw t;
        }
    }

    public long throwException(String a, Throwable t, long b) throws Throwable {
        try {
            String x = a + b;
            if (t == null)
                return b;
            throw t;
        } catch (Throwable ex) {
            ex.toString();
            throw t;
        }
    }

    public long throwExceptionOnly(String a, Throwable t, long b) throws Throwable {
        try {
            String x = a + b;
            if (t == null)
                return b;
            throw t;
        } catch (Throwable ex) {
            ex.toString();
            throw t;
        }
    }

    public long throwExceptionJustThrowable(String a, Throwable t, long b) throws Throwable {
        try {
            String x = a + b;
            if (t == null)
                return b;
            throw t;
        } catch (Throwable ex) {
            ex.toString();
            throw t;
        }
    }

    public long throwExceptionJustThrowable2(String a, Throwable t, long b) throws Throwable {
        try {
            String x = a + b;
            if (t == null)
                return b;
            throw t;
        } catch (Throwable ex) {
            ex.toString();
            throw t;
        }
    }
}
