package com.netcracker.profiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.netcracker.profiler.agent.LocalBuffer;
import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.Profiler;

import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LocalBufferTest {
    @Mocked
    Profiler unused = null;

    @Test
    public void isCleanOnConstruct() {
        LocalBuffer buffer = new LocalBuffer();
        assertEquals(0, buffer.first, "first");
        assertEquals(0, buffer.count, "count");
        assertNull(buffer.prevBuffer, "prevBuffer");
        assertNull(buffer.state, "state");
    }

    @BeforeAll
    public static void setupBufferSize() {
        System.setProperty("com.netcracker.profiler.util.LocalBuffer.SIZE", "10");
    }

    @Test
    public void enterSwitchWhenFull() {
        if (Integer.getInteger("com.netcracker.profiler.Profiler.minimal_logged_duration", 1) != 0)
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
        assertEquals(0, state.buffer.first, "Second buffer should contents start at 0");
        assertEquals(1, state.buffer.count, "Second buffer should contain 1 record");
    }


    @Test
    public void exitSwitchWhenFull() {
        if (Integer.getInteger("com.netcracker.profiler.Profiler.minimal_logged_duration", 1) != 0)
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
        assertEquals(0, state.buffer.first, "Second buffer should contents start at 0");
        assertEquals(1, state.buffer.count, "Second buffer should contain 1 record");
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
        assertEquals(0, state.buffer.first, "Second buffer should contents start at 0");
        assertEquals(1, state.buffer.count, "Second buffer should contain 1 record");
    }
}
