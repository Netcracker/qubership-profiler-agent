package com.netcracker.profiler.test.pigs;

/**
 * Tests adding fields, methods, etc
 */
public class ChangeStructurePig {
    public static String testClinit;

    public static boolean q = true;
    public static String w = "w";

    public void instanceMethod(int x) {
    }

    public static void staticMethod(int x) {
    }

    static {
        try {
            new String("abc").hashCode();
        } catch (Throwable t) {
            new String("def" + t.getMessage()).hashCode();
        }
    }
}
