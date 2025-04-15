package org.qubership.profiler.test;

import org.qubership.profiler.test.pigs.ExecuteMethodPig;
import org.qubership.profiler.test.util.Randomizer;

import mockit.FullVerifications;
import mockit.Mocked;
import mockit.VerificationsInOrder;
import org.testng.annotations.Test;

public class ExecuteMethodTest extends InitTransformers {
    @Mocked
    ExecuteMethodPig.Observer unused;

    final int a = Randomizer.rnd.nextInt();
    final byte b = (byte) Randomizer.rnd.nextInt();
    final short c = (short) Randomizer.rnd.nextInt();
    final long d = Randomizer.rnd.nextLong();
    final double e = Randomizer.rnd.nextDouble();
    final float f = Randomizer.rnd.nextFloat();
    final int[] h = new int[]{Randomizer.rnd.nextInt(), Randomizer.rnd.nextInt()};
    final byte[] i = new byte[]{(byte) Randomizer.rnd.nextInt(), (byte) Randomizer.rnd.nextInt()};
    final Integer g = Randomizer.rnd.nextInt();
    final String[] k = new String[]{Randomizer.randomString()};

    @Test
    public void executeBefore() {
        ExecuteMethodPig.staticExecuteBefore(a, b, c, d, e, f, h, i, g, k);
        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.staticMethod2(a, b, c, d, e, f, h, i, g, k);
                ExecuteMethodPig.Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeAfter() {
        ExecuteMethodPig.staticExecuteAfter(a, b, c, d, e, f, h, i, g, k);
        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
                ExecuteMethodPig.Observer.staticMethod2(a, b, c, d, e, f, h, i, g, k);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeAfterWithResult() {
        new ExecuteMethodPig().staticExecuteAfterWithResult(a, b, c, d, e, f, h, i, g, k);
        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.staticMethod1(a, b, c, d, e, f, h, i, g, k);
                ExecuteMethodPig.Observer.staticMethod2(a, b, c, d, e, f, h, i, g, k);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeInstead() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        new ExecuteMethodPig().executeInstead(a, b);

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.executeInstead2(a, b);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeWhenExceptionOnlyThrows() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = new Throwable();
        try {
            new ExecuteMethodPig().throwExceptionOnly(a, t, b);
        } catch (Throwable ex) {
            /* ignore */
        }

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwExceptionOnly(a, t, b, t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeWhenExceptionOnlyDoesNotThrow() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = null;
        new ExecuteMethodPig().throwExceptionOnly(a, t, b);

        new VerificationsInOrder() {
            {
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeWhenExceptionThrows() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = new Throwable();
        try {
            new ExecuteMethodPig().throwException(a, t, b);
        } catch (Throwable ex) {
            /* ignore */
        }

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwException(a, t, b, t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void executeWhenExceptionDoesNotThrow() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = null;
        new ExecuteMethodPig().throwException(a, t, b);

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwException(a, t, b, t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void throwExceptionJustThrowable() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = new Throwable();
        try {
            new ExecuteMethodPig().throwExceptionJustThrowable(a, t, b);
        } catch (Throwable ex) {
            /* ignore */
        }

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwExceptionJustThrowable(t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void throwExceptionJustThrowableDoesNotThrow() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = null;
        new ExecuteMethodPig().throwExceptionJustThrowable(a, t, b);

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwExceptionJustThrowable(t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void throwExceptionJustThrowable2() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = new Throwable();
        try {
            new ExecuteMethodPig().throwExceptionJustThrowable2(a, t, b);
        } catch (Throwable ex) {
            /* ignore */
        }

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwExceptionJustThrowable(t);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void throwExceptionJustThrowableDoesNotThrow2() throws Throwable {
        final String a = Randomizer.randomString();
        final long b = Randomizer.rnd.nextLong();
        final Throwable t = null;
        new ExecuteMethodPig().throwExceptionJustThrowable2(a, t, b);

        new VerificationsInOrder() {
            {
                ExecuteMethodPig.Observer.throwExceptionJustThrowable(t);
            }
        };
        new FullVerifications(){};
    }
}
