package com.netcracker.profiler.instrument.custom.impl;

import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

public class KernelSecurityProcessorHandler extends MethodInstrumenter implements Opcodes {
    private final static Type C_SECURITY_PROCESSOR = Type.getObjectType("com/netcracker/ejb/session/security/SecurityProcessor");
    private final static Method M_GET_CURRENT_USER_ID = Method.getMethod("String getCurrentUserId$profiler()");
    private final static Method M_SAVE_USER_ID = Method.getMethod("void saveUserId$profiler(String)");

    int userNameVariableNumber;

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        userNameVariableNumber = ma.newLocal(Type.getType(String.class));
        ma.invokeStatic(C_SECURITY_PROCESSOR, M_GET_CURRENT_USER_ID);
        ma.storeLocal(userNameVariableNumber);
    }

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {}

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        ma.loadLocal(userNameVariableNumber);
        ma.invokeStatic(C_SECURITY_PROCESSOR, M_SAVE_USER_ID);
    }

    @Override
    public void onMethodException(ProfileMethodAdapter ma) {
        onMethodExit(ma);
    }
}
