package com.netcracker.profiler.instrument.custom.util;

import com.netcracker.profiler.instrument.ProfileMethodAdapter;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteMethodInstead extends ExecuteMethod {
    public static final Logger log = LoggerFactory.getLogger(ExecuteMethodInstead.class);

    public ExecuteMethodInstead() {
        this.addTryCatchBlocks = false;
    }

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {
        super.onMethodEnter(ma);
        appendCall(ma);
        Object[] locals = ma.getMethodArgsAsLocals();
        // Frame is required for java 1.7 verifier
        ma.visitFrame(Opcodes.F_NEW, locals.length, locals, 0, EMPTY_STACK);
    }

    @Override
    protected void storeResult(ProfileMethodAdapter ma, Type returnType) {
        ma.returnValue();
    }
}
