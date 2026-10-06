package com.netcracker.profiler.cloud.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ESCStopWatchTest {
    long ONE = 1000000L;
    long TWO = 2L*1000000L;
    long THREE = 3L*1000000L;
    long FIVE = 5L*1000000L;
    long SEVEN = 7L*1000000L;

    @Test
    public void test(){
        final long[] timeMock = new long[]{System.nanoTime()};
        ESCStopWatch pig = new ESCStopWatch(){
            @Override
            protected long nowNanos() {
                return timeMock[0];
            }
        };

        pig.start();
        timeMock[0] += ONE;
        pig.stop();

        assertEquals(pig.getAndReset(), ONE);

        pig.start();
        timeMock[0] += ONE;
        pig.stop();
        timeMock[0] += TWO;
        pig.start();
        timeMock[0] += THREE;
        pig.stop();

        assertEquals(ONE+THREE, pig.getAndReset());

        pig.start();
        timeMock[0] += FIVE;
        assertEquals(FIVE, pig.getAndReset());
    }
}
