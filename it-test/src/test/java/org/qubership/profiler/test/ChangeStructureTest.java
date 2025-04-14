package org.qubership.profiler.test;

import org.qubership.profiler.test.pigs.ChangeStructurePig;
import org.qubership.profiler.test.pigs.ChildChangeStructurePig;
import org.qubership.profiler.test.pigs.TransactionPig;
import org.qubership.profiler.test.util.Randomizer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ChangeStructureTest extends InitTransformers {
    ChangeStructurePig pig = new ChangeStructurePig();

    @Test
    public void instanceMethod() {
        final int x = Randomizer.rnd.nextInt();
        final int y = Randomizer.rnd.nextInt();

        int result = ReflectionTestUtils.invokeMethod(pig, "addedMethod$profiler", x, y);
        Assert.assertEquals(result, x + y);
    }

    @Test
    public void staticMethod() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final int x = Randomizer.rnd.nextInt();
        final int y = Randomizer.rnd.nextInt();
        Method add = ChangeStructurePig.class.getMethod("addedStaticMethod$profiler", int.class, int.class);
        int result = (int) add.invoke(null, x, y);
        Assert.assertEquals(result, x + y);
    }

    @Test
    public void addedInstanceField() throws NoSuchFieldException {
        Field f = pig.getClass().getDeclaredField("addedInstanceField$profiler");
        Assert.assertEquals(f.getType(), double.class, "addedInstanceField$profiler should be of double type");
    }

    @Test
    public void addedStaticField() throws NoSuchFieldException {
        Field f = pig.getClass().getDeclaredField("addedStaticField$profiler");
        Assert.assertEquals(f.getType(), String.class, "addedStaticField$profiler should be of String type");
    }

    @Test
    public void addedClinit() {
        Assert.assertEquals(ChangeStructurePig.testClinit, "field was initialized by Profiler");
    }

    @Test
    public void addedMethod() {
        String test = "test";
        ChildChangeStructurePig ccsp = new ChildChangeStructurePig(test);
        String result = ReflectionTestUtils.invokeMethod(pig, "addedMethodRunnable$profiler", ccsp);
        Assert.assertEquals(result, test);
    }

    @Test
    public void transactionTest() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("test"));
        //pig.initCause(new IllegalArgumentException("via initCause"));
        String string = pig.getStatusAsString();
        System.out.println("string = " + string);
        Assert.assertTrue(string.startsWith("Marked rollback."), "Status 1 should mean 'marked rollback'");
        Assert.assertTrue(string.contains("IllegalStateException"), "Message should include rollback reason");
    }

    @Test
    public void transactionTestInitCause() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("test"));
        pig.initCause(new IllegalArgumentException("via initCause"));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pig.printStackTrace(pw);
        String res = sw.toString();
        Assert.assertTrue(res.contains("via initCause"), "printStackTrace prints cause exception");
        System.out.println("Result of printStackTrace (whould include caused by) = " + res);
    }

    @Test
    public void transactionTestConstructor() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("via constructor"));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pig.printStackTrace(pw);
        String res = sw.toString();
        Assert.assertTrue(res.contains("via constructor"), "printStackTrace prints cause exception");
        System.out.println("Result of printStackTrace (whould include caused by) = " + res);
    }
}
