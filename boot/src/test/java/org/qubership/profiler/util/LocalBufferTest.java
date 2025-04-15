package org.qubership.profiler.util;

import org.qubership.profiler.agent.LocalBuffer;
import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.Profiler;

import mockit.Mocked;
import mockit.Verifications;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LocalBufferTest {
    @Mocked
    Profiler unused = null;

    @Test
    public void isCleanOnConstruct() {
        LocalBuffer buffer = new LocalBuffer();
        Assert.assertEquals(buffer.first, 0, "first");
        Assert.assertEquals(buffer.count, 0, "count");
        Assert.assertEquals(buffer.prevBuffer, null, "prevBuffer");
        Assert.assertEquals(buffer.state, null, "state");
    }

    @BeforeClass
    public static void setupBufferSize() {
        System.setProperty("org.qubership.profiler.util.LocalBuffer.SIZE", "10");
    }

    @Test
    public void enterSwitchWhenFull() {
        if (Integer.getInteger("org.qubership.profiler.Profiler.minimal_logged_duration", 1) != 0)
            return;
        final LocalBuffer buffer = new LocalBuffer();
        LocalState state = new LocalState();
        buffer.state = state;
        state.buffer = new LocalBuffer();
        for (int i = 0; i < LocalBuffer.SIZE; i++)
            buffer.initEnter(0);

        new Verifications() {
            {
                Profiler.exchangeBuffer((LocalBuffer) any);
                times = 0;
            }
        };

        buffer.initEnter(0);

        new Verifications() {
            {
                Profiler.exchangeBuffer(withSameInstance(buffer));
                times = 1;
            }
        };
        Assert.assertEquals(state.buffer.first, 0, "Second buffer should contents start at 0");
        Assert.assertEquals(state.buffer.count, 1, "Second buffer should contain 1 record");
    }


    @Test
    public void exitSwitchWhenFull() {
        if (Integer.getInteger("org.qubership.profiler.Profiler.minimal_logged_duration", 1) != 0)
            return;
        final LocalBuffer buffer = new LocalBuffer();
        LocalState state = new LocalState();
        buffer.state = state;
        state.buffer = new LocalBuffer();

        for (int i = 0; i < LocalBuffer.SIZE; i++)
            buffer.initExit();

        new Verifications() {
            {
                Profiler.exchangeBuffer((LocalBuffer) any);
                times = 0;
            }
        };

        buffer.initExit();

        new Verifications() {
            {
                Profiler.exchangeBuffer(withSameInstance(buffer));
                times = 1;
            }
        };
        Assert.assertEquals(state.buffer.first, 0, "Second buffer should contents start at 0");
        Assert.assertEquals(state.buffer.count, 1, "Second buffer should contain 1 record");
    }

    @Test
    public void eventSwitchWhenFull() {
        final LocalState state = new LocalState();
        final LocalBuffer buffer = new LocalBuffer();
        buffer.state = state;
        state.buffer = new LocalBuffer();

        for (int i = 0; i < LocalBuffer.SIZE; i++)
            buffer.event("a", 0);

        new Verifications() {
            {
                Profiler.exchangeBuffer((LocalBuffer) any);
                times = 0;
            }
        };

        buffer.event("a", 0);

        new Verifications() {
            {
                Profiler.exchangeBuffer(withSameInstance(buffer));
                times = 1;
            }
        };
        Assert.assertEquals(state.buffer.first, 0, "Second buffer should contents start at 0");
        Assert.assertEquals(state.buffer.count, 1, "Second buffer should contain 1 record");
    }
}
