package com.netcracker.profiler.agent;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.ArrayList;

public class ClassPreProcessor {
    private static final ESCLogger logger = ESCLogger.getLogger(ClassPreProcessor.class);
    private static ArrayList<ClassFileTransformer> transformers;

    public ClassPreProcessor() {
        InstrumentationImplForJava14 inst = new InstrumentationImplForJava14();
        transformers = inst.getTransformers();
        Bootstrap.premain(null, inst);
   }

    public final byte[] defineClass0Pre(ClassLoader caller,
                                         String name,
                                         byte[] b,
                                         int off,
                                         int len,
                                         ProtectionDomain pd) {
        final ArrayList<ClassFileTransformer> transformers = ClassPreProcessor.transformers;
        if (transformers != null)
            try {
                byte[] ibyte;
                if (off == 0 && len == b.length) {
                    ibyte = b;
                } else {
                    ibyte = new byte[len];
                    System.arraycopy(b, off, ibyte, 0, len);
                }
                for (int i = 0; i < transformers.size(); i++) {
                    final ClassFileTransformer fileTransformer = transformers.get(i);
                    try {
                        byte[] newBytes = fileTransformer.transform(caller, name.replace('.', '/'), null, pd, ibyte);
                        if (newBytes != null) ibyte = newBytes;
                    } catch (Throwable e) {
                        logger.severe("Unable to transform class " + name + " using " + fileTransformer.getClass().getName(), e);
                    }
                }
                return ibyte;
            } catch (Throwable throwable) {
                logger.severe(
                        "Error pre-processing class "
                                + name
                                + " in "
                                + Thread.currentThread(),
                        throwable
                );
            }
        byte[] obyte = new byte[len];
        System.arraycopy(b, off, obyte, 0, len);
        return obyte;
    }
}
