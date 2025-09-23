package com.netcracker.profiler.test.pigs;

import com.netcracker.profiler.agent.TimerCache;

/**
 * This class will be
 */
public class LogParameterPig {
    public void stringArg(String a) {
    }

    public void intArg(int a) {
    }

    public static void staticInt(int a) {
    }

    public static void throwsException(int a) throws InterruptedException {
        shiftTime(a);
        throw new RuntimeException("throwsException: " + a);
    }

    public static void staticIntDouble(int a, double b) {
    }

    public static void shiftsTime(int a, double b) {
        shiftTime(1000);
    }

    public double returnsDouble(double a, int b) {
        if (a * a > 0)
            shiftTime(1000);
        else
            shiftTime(999);
        return a * b;
    }

    public byte returnsByte(byte x, byte y) {
        if (x + y > 0)
            shiftTime(1000);
        else
            shiftTime(999);
        return (byte) (x + y);
    }

    private static void shiftTime(int delta) {
        long now = TimerCache.now + delta;
        int timer = (int) (now - TimerCache.startTime);
        TimerCache.timer = timer;
        TimerCache.timerSHL32 = ((long) timer) << 32;
        TimerCache.timerWithoutSuspend = (int) (now - TimerCache.startTime);
    }

    static class A {
        private final int x;

        public A(int x) {
            this.x = x;
        }
    }

    static class B extends A {
        public B(int x) {
            super(x < 10000 ? x : (x < 100 ? x + new String("sdfsadf").hashCode() : x));
            try {
                "a".hashCode();
            } catch (Throwable e) {
                throw new RuntimeException("unable to construct B(" + x + ")", e);
            }
        }
    }

    public void invokesConstructor(int x) {
        new B(x);
    }
}
