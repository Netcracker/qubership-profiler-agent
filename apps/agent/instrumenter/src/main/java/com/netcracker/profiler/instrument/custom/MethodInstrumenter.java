package com.netcracker.profiler.instrument.custom;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;

import org.w3c.dom.Element;

public abstract class MethodInstrumenter implements MethodAcceptor {
    protected final static Object[] EMPTY_STACK = new Object[0];

    public void declareLocals(ProfileMethodAdapter ma) {
    }

    /**
     * This method is called when instrumented method begins
     *
     * @param ma method adapter that should be used to append bytecode
     */
    public void onMethodEnter(ProfileMethodAdapter ma) {
    }

    /**
     * This method is called only for normal exit from the method.
     *
     * @param ma method adapter that should be used to append bytecode
     */
    public void onMethodExit(ProfileMethodAdapter ma) {
    }

    public MethodInstrumenter init(Element e, Configuration_01 Configuration) {
        return this;
    }

    public void onMethodException(ProfileMethodAdapter ma){
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return getClass() == obj.getClass();
    }
}
