package org.qubership.profiler.test;

import org.qubership.profiler.test.pigs.ProcessArgumentPig;
import org.qubership.profiler.test.pigs.TestRes;

import mockit.FullVerifications;
import mockit.Mocked;
import mockit.VerificationsInOrder;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

public class ProcessArgumentTest extends InitTransformers {
    @Mocked
    ProcessArgumentPig.Observer unused;

    ProcessArgumentPig pig = new ProcessArgumentPig();

    @Test
    public void processInt() {
        int res = pig.intArg(42);
        Assert.assertEquals(42 + 3 + 7, res);
        new VerificationsInOrder() {
            {
                int arg;
                ProcessArgumentPig.Observer.intArg(arg = withCapture());
                Assert.assertEquals(49, arg);
            }
        };
        new FullVerifications(){};
    }

    @Test
    public void processMap() {
        HashMap x = new HashMap();
        x.put("test", "y");
        x.put("x", "y");
        String res = pig.mapArg(x);
        Assert.assertEquals("result", "y", res);
        new VerificationsInOrder() {
            {
                String arg;
                ProcessArgumentPig.Observer.objectArg(arg = withCapture());
                Assert.assertEquals("Observer.objectArg", "after process", arg);
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
