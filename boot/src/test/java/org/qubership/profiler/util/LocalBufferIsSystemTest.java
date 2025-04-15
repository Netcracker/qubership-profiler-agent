package org.qubership.profiler.util;

import org.qubership.profiler.agent.LocalBuffer;
import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;

import mockit.Mocked;
import mockit.Tested;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LocalBufferIsSystemTest {
    @Tested
    @Mocked
    final ProfilerData unused = null;

    @Tested
    @Mocked()
    final Profiler unused2 = null;

    @BeforeClass
    public void setupBufferSize() {
        org.qubership.profiler.util.LocalBufferTest.setupBufferSize();
    }

    @Test
    public void systemBufferDoesNotSwitch() {
//        new Expectations() {
//            {
//                MethodReflection.invoke(ProfilerData.class, (Object) null, "addDirtyBuffer", LocalBuffer.class);
//                times = 0;
//                MethodReflection.invoke(ProfilerData.class, (Object) null, "addEmptyBuffer", LocalBuffer.class);
//                times = 0;
//                MethodReflection.invoke(ProfilerData.class, (Object) null, "getEmptyBuffer", LocalState.class);
//                times = 0;
//            }
//        };

        final LocalState state = new LocalState();
        final LocalBuffer buffer = new LocalBuffer();
        buffer.state = state;
        state.buffer = buffer;
        state.markSystem();

        for (int i = 0; i < 2 * LocalBuffer.SIZE; i++)
            state.event("a", 1);
    }
}
