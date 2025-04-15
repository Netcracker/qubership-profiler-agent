package org.qubership.profiler.test.util;

import org.qubership.profiler.instrument.TypeUtils;

import org.objectweb.asm.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassNamingTest {
    public void simpleMethod(){}

    public String simpleMethodReturningString(){ return null; }

    public String methodTakingArray(int[] a){ return null; }

    public StringBuffer methodTakingArrayAndString(int[] a, String b){ return null; }

    @Test
    public void testSimpleClassName() throws NoSuchMethodException {
        Class clazz = getClass();
        String formatted;

        formatted = TypeUtils.getMethodFullname("simpleMethod", Type.getMethodDescriptor(clazz.getDeclaredMethod("simpleMethod")), clazz.getName(), "ClassNamingTest.java", 10, "test.jar");
        Assert.assertEquals(formatted, "void "+clazz.getName()+".simpleMethod() (ClassNamingTest.java:10) [test.jar]");

        formatted = TypeUtils.getMethodFullname("simpleMethodReturningString", Type.getMethodDescriptor(clazz.getDeclaredMethod("simpleMethodReturningString")), clazz.getName(), "ClassNamingTest.java", 20, "test.jar");
        Assert.assertEquals(formatted, "java.lang.String "+clazz.getName()+".simpleMethodReturningString() (ClassNamingTest.java:20) [test.jar]");

        formatted = TypeUtils.getMethodFullname("methodTakingArray", Type.getMethodDescriptor(clazz.getDeclaredMethod("methodTakingArray", int[].class)), clazz.getName(), "ClassNamingTest.java", 30, "test.jar");
        Assert.assertEquals(formatted, "java.lang.String "+clazz.getName()+".methodTakingArray(int[]) (ClassNamingTest.java:30) [test.jar]");

        formatted = TypeUtils.getMethodFullname("methodTakingArrayAndString", Type.getMethodDescriptor(clazz.getDeclaredMethod("methodTakingArrayAndString", int[].class, String.class)), clazz.getName(), "ClassNamingTest.java", 40, "test.jar");
        Assert.assertEquals(formatted, "java.lang.StringBuffer "+clazz.getName()+".methodTakingArrayAndString(int[],java.lang.String) (ClassNamingTest.java:40) [test.jar]");
    }
}
