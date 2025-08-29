package org.qubership.profiler.instrument.enhancement;

import org.objectweb.asm.ClassVisitor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectedEnhancerBridge implements ClassEnhancer {
    Method target;

    public ReflectedEnhancerBridge(Class klass, String method) {
        try {
            target = klass.getMethod(method, ClassVisitor.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Unable to find method " + method + " in class " + klass, e);
        }
    }

    @SuppressWarnings("CatchAndPrintStackTrace")
    public void enhance(ClassVisitor cv) {
        try {
            target.invoke(null, cv);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.getCause().printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "ReflectedEnhancerBridge{" +
                "target=" + target +
                '}';
    }
}
