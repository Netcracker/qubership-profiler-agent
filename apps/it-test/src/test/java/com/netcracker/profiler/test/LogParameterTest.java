package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.agent.DumperConstants;
import com.netcracker.profiler.agent.LocalBuffer;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.test.pigs.LogParameterPig;
import com.netcracker.profiler.test.util.Randomizer;

import mockit.Capturing;
import mockit.Expectations;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests all aspects of class profiling/instrumentation
 */
public class LogParameterTest extends InitTransformers {
    @Capturing
    private LocalBuffer buffer;

    @Test
    public void stringArg() {
        LogParameterPig pig = new LogParameterPig();
        final String x = Randomizer.randomString();
        final int tagId = ProfilerData.resolveTag("parameter.string") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(x, tagId);
            times = 1;
        }};

        pig.stringArg(x);
    }

    @Test
    public void intArg() {
        LogParameterPig pig = new LogParameterPig();
        final int x = Randomizer.rnd.nextInt();
        final int tagId = ProfilerData.resolveTag("parameter.int") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(x), tagId);
            times = 1;
        }};

        pig.intArg(x);
    }

    @Test
    public void staticInt() {
        final int x = Randomizer.rnd.nextInt();
        final int tagId = ProfilerData.resolveTag("parameter.staticInt") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(x), tagId);
            times = 1;
        }};

        LogParameterPig.staticInt(x);
    }

    @Test
    public void staticIntDouble() {
        final int vInt = Randomizer.rnd.nextInt();
        final int tagInt = ProfilerData.resolveTag("parameter.staticInt") | DumperConstants.DATA_TAG_RECORD;
        final double vDouble = Randomizer.rnd.nextDouble();
        final int tagDouble = ProfilerData.resolveTag("parameter.staticDouble") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(vInt), tagInt);
            times = 1;
            buffer.event(String.valueOf(vDouble), tagDouble);
            times = 1;
        }};

        LogParameterPig.staticIntDouble(vInt, vDouble);
    }

    @Test
    @Disabled
    public void logWhenDurationExceeds() {
        final int vInt = Randomizer.rnd.nextInt();
        final int tagInt = ProfilerData.resolveTag("parameter.int") | DumperConstants.DATA_TAG_RECORD;
        final double vDouble = Randomizer.rnd.nextDouble();
        final int tagDouble = ProfilerData.resolveTag("parameter.double") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(vInt), tagInt);
            times = 0;
            buffer.event(String.valueOf(vDouble), tagDouble);
            times = 1;
        }};

        LogParameterPig.shiftsTime(vInt, vDouble);
    }

    @Test
    public void invokesConstructor() {
        LogParameterPig pig = new LogParameterPig();
        final int x = Randomizer.rnd.nextInt();
        final int tagId = ProfilerData.resolveTag("parameter.int") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(x), tagId);
            times = 1;
        }};

        pig.invokesConstructor(x);
    }

    @Test
    public void logReturnDouble() {
        LogParameterPig pig = new LogParameterPig();
        final int vInt = Randomizer.rnd.nextInt();
        final double vDouble = Randomizer.rnd.nextDouble();
        final int tagDouble500 = ProfilerData.resolveTag("return.double500") | DumperConstants.DATA_TAG_RECORD;
        final int tagDouble2000 = ProfilerData.resolveTag("return.double2000") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf(vDouble * vInt), tagDouble500);
            times = 1;
            buffer.event(String.valueOf(vDouble * vInt), tagDouble2000);
            times = 0;
        }};

        double result = pig.returnsDouble(vDouble, vInt);
        assertEquals(vDouble * vInt, result, "Resulting value do not match a*b");
    }

    @Test
    public void logReturnByte() {
        LogParameterPig pig = new LogParameterPig();
        final byte vByte1 = (byte) Randomizer.rnd.nextInt();
        final byte vByte2 = (byte) Randomizer.rnd.nextInt();
        final int tagByte500 = ProfilerData.resolveTag("return.byte500") | DumperConstants.DATA_TAG_RECORD;
        final int tagByte2000 = ProfilerData.resolveTag("return.byte2000") | DumperConstants.DATA_TAG_RECORD;

        new Expectations() {{
            buffer.event(String.valueOf((byte) (vByte1 + vByte2)), tagByte500);
            times = 1;
            buffer.event(String.valueOf((byte) (vByte1 + vByte2)), tagByte2000);
            times = 0;
        }};

        byte result = pig.returnsByte(vByte1, vByte2);
        assertEquals((byte)(vByte1 + vByte2), result, "Resulting byte do not match a+b");
    }

}
