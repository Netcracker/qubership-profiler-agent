package com.netcracker.profiler.test;

import com.netcracker.profiler.agent.TimerCache;

public class MethodCodeTest {
    public void test1() {
        String a = new Object().toString();
        String b = new Object().toString();
        String c = a + b;
    }

    static String test2() {
        String a = new Object().toString();
        String b = new Object().toString();
        String c = a + b;
        return c;
    }

    public String test3(String a) {
        String b = new Object().toString();
        String c = a + b;
        return c;
    }

    public String test3(String a, String b) {
        String c = a + b;
        return c;
    }

    public String test_re(String a, String b) {
        try {
            String c = a + b;
            return c;
        } catch (RuntimeException re) {
            re.printStackTrace();
        }
        return "abcd";
    }

    public String test_nf_re(String a, String b) {
        String c = null;
        try {
            c = a + b;
            return c;
        } catch (NumberFormatException nf) {
            nf.printStackTrace();
        } catch (RuntimeException re) {
            re.printStackTrace();
        }
        return c;
    }

    public void test(String x) {
        System.out.println("x  =  " + x);
    }

    public String test_nf_re_fin(String a, String[] b, boolean d, short q, int y, double[][] z) {
        test(a);
        System.out.println("test_nf_re_fin2( " + a + ", " + b + ")");
        String c = null;
        try {
            c = a + b;
            return c;
        } catch (NumberFormatException nf) {
            nf.printStackTrace();
        } catch (RuntimeException re) {
            re.printStackTrace();
        } finally {
            c += "defg";
        }
        return c;
    }

    public static void main(String[] args) {
        double abc = 123;
        System.out.println("MethocCodeTest.main");
        MethodCodeTest test = new MethodCodeTest();
        if (args != null)
            test.test_nf_re_fin("a", new String[]{"b"}, false, (short) 0, 0, null);
        System.out.println("abc =" + abc);
    }
    public static void added(){

    }
    public static void timerTest() throws Throwable {
        int timer = TimerCache.timer;
        try {
            if (TimerCache.timer-timer>0)
                added();
        } catch (Throwable t){
            throw t;
        }
    }
}
