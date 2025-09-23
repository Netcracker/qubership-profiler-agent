package com.netcracker.profiler.agent;

import java.security.ProtectionDomain;

public class ClassPreProcessorHelper {
    volatile static ClassPreProcessor preProcessor = new ClassPreProcessor();

    /**
     * byte code instrumentation of class loaded
     */
    public static byte[] defineClass0Pre(ClassLoader caller,
                                         String name,
                                         byte[] b,
                                         int off,
                                         int len,
                                         ProtectionDomain pd) {
        if (preProcessor == null){
            if (off == 0 && b.length == len)
                return b;
            byte[] obyte = new byte[len];
            System.arraycopy(b, off, obyte, 0, len);
            return obyte;
        }
        return preProcessor.defineClass0Pre(caller, name, b, off, len, pd);
    }
}
