package org.qubership.profiler.test.pigs;

public class ChangeStructurePig {
    native void instanceMethod(int x);

    native static void staticMethod(int x);

    private double addedInstanceField$profiler;

    private static String addedStaticField$profiler;

    public static int addedStaticMethod$profiler(int x, int y) throws Throwable {
        try {
            try {
                "some logic".toCharArray();
            } catch (NumberFormatException e) {
                throw new IllegalStateException("x=" + x, e);
            } finally {
                if ("fuzzy".hashCode() < "logic".hashCode() || !new String("fuzzy").equals(new String("logic"))) {
                    try {
                        new Throwable("hi").hashCode();
                    } finally {
                        "abc".toString();
                        staticMethod(x); // This is a main aim of added method that is controlled in unittest
                    }
                }
            }
            return x + y;
        } catch (Throwable t) {
            throw new RuntimeException("addedStaticMethod$profiler(" + x + ")", t);
        }
    }

    @SuppressWarnings("Finally")
    public int addedMethod$profiler(int x, int y) throws Throwable {
        try {
            try {
                "some logic".toCharArray();
            } catch (NumberFormatException e) {

            } finally {
                if ("fuzzy".hashCode() < "logic".hashCode() || !new String("fuzzy").equals(new String("logic"))) {
                    try {
                        if (System.currentTimeMillis() < 0)
                            throw new Throwable("hi");
                        else
                            instanceMethod(x); // This is a main aim of added method that is controlled in unittest
                    } finally {
                        addedInstanceField$profiler = x;
                        "abc".toString();
                    }
                }
            }
            return x + y;
        } catch (Throwable t) {
            throw new RuntimeException("e", t);
        }
    }

    public static String testClinit;

    static {
        testClinit = "field was initialized by Profiler";
    }

    public static String getValueFromACSP$profiler(Runnable pig) {
        if (pig instanceof AbstractChangeStructurePig)
            return ((AbstractChangeStructurePig) pig).toString$profiler();
        return null;
    }

    public String addedMethodRunnable$profiler(Runnable pig) {
        return getValueFromACSP$profiler(pig);
    }
}
