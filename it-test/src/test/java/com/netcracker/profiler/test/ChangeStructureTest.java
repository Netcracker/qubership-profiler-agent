package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.netcracker.profiler.test.pigs.ChangeStructurePig;
import com.netcracker.profiler.test.pigs.ChildChangeStructurePig;
import com.netcracker.profiler.test.pigs.TransactionPig;
import com.netcracker.profiler.test.util.Randomizer;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ChangeStructureTest extends InitTransformers {
    @Test
    public void instanceMethod() {
        ChangeStructurePig pig = new ChangeStructurePig();
        final int x = Randomizer.rnd.nextInt();
        final int y = Randomizer.rnd.nextInt();

        int result = ReflectionTestUtils.invokeMethod(pig, "addedMethod$profiler", x, y);
        assertEquals(result, x + y);
    }

    @Test
    public void staticMethod() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final int x = Randomizer.rnd.nextInt();
        final int y = Randomizer.rnd.nextInt();
        Method add = ChangeStructurePig.class.getMethod("addedStaticMethod$profiler", int.class, int.class);
        int result = (int) add.invoke(null, x, y);
        assertEquals(x + y, result);
    }

    @Test
    public void addedInstanceField() throws NoSuchFieldException {
        ChangeStructurePig pig = new ChangeStructurePig();
        Field f = pig.getClass().getDeclaredField("addedInstanceField$profiler");
        assertEquals(double.class, f.getType(), "addedInstanceField$profiler should be of double type");
    }

    @Test
    public void addedStaticField() throws NoSuchFieldException {
        ChangeStructurePig pig = new ChangeStructurePig();
        Field f = pig.getClass().getDeclaredField("addedStaticField$profiler");
        assertEquals(String.class, f.getType(), "addedStaticField$profiler should be of String type");
    }

    @Test
    public void addedClinit() {
        assertEquals("field was initialized by Profiler", ChangeStructurePig.testClinit);
    }

    @Test
    public void addedMethod() {
        ChangeStructurePig pig = new ChangeStructurePig();
        String test = "test";
        ChildChangeStructurePig ccsp = new ChildChangeStructurePig(test);
        String result = ReflectionTestUtils.invokeMethod(pig, "addedMethodRunnable$profiler", ccsp);
        assertEquals(test, result);
    }

    @Test
    public void transactionTest() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("test"));
        //pig.initCause(new IllegalArgumentException("via initCause"));
        String string = pig.getStatusAsString();
        assertTrue(string.startsWith("Marked rollback."), "Status 1 should mean 'marked rollback'");
        assertTrue(string.contains("IllegalStateException"), "Message should include rollback reason");
    }

    @Test
    public void transactionTestInitCause() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("test"));
        pig.initCause(new IllegalArgumentException("via initCause"));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pig.printStackTrace(pw);
        String res = sw.toString();
        assertTrue(res.contains("via initCause"), "printStackTrace prints cause exception");
    }

    @Test
    public void transactionTestConstructor() {
        TransactionPig pig = new TransactionPig(1, new IllegalStateException("via constructor"));

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pig.printStackTrace(pw);
        String res = sw.toString();
        assertTrue(res.contains("via constructor"), "printStackTrace prints cause exception");
    }
}
