package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.test.pigs.ProcessArgumentPig;
import com.netcracker.profiler.test.pigs.TestRes;

import mockit.FullVerifications;
import mockit.Mocked;
import mockit.VerificationsInOrder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class ProcessArgumentTest extends InitTransformers {
    @Mocked
    ProcessArgumentPig.Observer unused;

    @Test
    public void processInt() {
        ProcessArgumentPig pig = new ProcessArgumentPig();
        int res = pig.intArg(42);
        assertEquals(42 + 3 + 7, res);
        new VerificationsInOrder() {
            {
                int arg;
                ProcessArgumentPig.Observer.intArg(arg = withCapture());
                assertEquals(49, arg);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void processMap() {
        ProcessArgumentPig pig = new ProcessArgumentPig();
        HashMap x = new HashMap();
        x.put("test", "y");
        x.put("x", "y");
        String res = pig.mapArg(x);
        assertEquals("y", res, "result");
        new VerificationsInOrder() {
            {
                String arg;
                ProcessArgumentPig.Observer.objectArg(arg = withCapture());
                assertEquals("after process", arg, "Observer.objectArg");
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void processR() {
        TestRes testRes = new TestRes("3");
        System.out.println(testRes);
    }
}
