package com.netcracker.profiler.agent;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.*;

public class ProfilerMetafactory {
    private static final MethodHandle PROFILER_PLUGIN_EXCEPTION_MH;

    static {
        try {
            PROFILER_PLUGIN_EXCEPTION_MH =
                    MethodHandles.publicLookup().findStatic(Profiler.class, "pluginException", methodType(void.class, Throwable.class));
        } catch (NoSuchMethodException|IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static CallSite catchPluginException(MethodHandles.Lookup lookup,
                                                String methodName,
                                                MethodType methodType,
                                                MethodHandle methodHandle) {
        MethodHandle profilerPluginExceptionMh;
        if(methodType.returnType().equals(void.class)) {
            profilerPluginExceptionMh = PROFILER_PLUGIN_EXCEPTION_MH;
        } else {
            profilerPluginExceptionMh = MethodHandles.filterReturnValue(PROFILER_PLUGIN_EXCEPTION_MH,
                    MethodHandles.constant(methodType.returnType(), getDefaultReturnValue(methodType.returnType())));
        }
        MethodHandle handleExceptionMH = MethodHandles.catchException(methodHandle, Throwable.class, profilerPluginExceptionMh);
        return new ConstantCallSite(handleExceptionMH);
    }

    private static Object getDefaultReturnValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        } else if (returnType.equals(byte.class)) {
            return 0;
        } else if (returnType.equals(char.class)) {
            return 0;
        } else if (returnType.equals(double.class)) {
            return 0;
        } else if (returnType.equals(float.class)) {
            return 0;
        } else if (returnType.equals(int.class)) {
            return 0;
        } else if (returnType.equals(long.class)) {
            return 0;
        } else if (returnType.equals(short.class)) {
            return 0;
        } else if (returnType.equals(boolean.class)) {
            return false;
        } else {
            throw new IllegalArgumentException("Invalid return type");
        }
    }

}
