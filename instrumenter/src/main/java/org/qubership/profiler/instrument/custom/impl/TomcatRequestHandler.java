package org.qubership.profiler.instrument.custom.impl;

import org.qubership.profiler.instrument.ProfileMethodAdapter;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomcatRequestHandler extends HttpServletRequest_run {
    public static final Logger log = LoggerFactory.getLogger(TomcatRequestHandler.class);

    private final static Type c_StandardEngineValve = Type.getObjectType("org/apache/catalina/core/StandardEngineValve");

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        ma.saveArg(0);
    }

    @Override
    public void loadRequestObject(ProfileMethodAdapter ma) {
        ma.loadLocal(ma.saveArg(0));
    }

    @Override
    protected Type getTargetClass() {
        return c_StandardEngineValve;
    }
}
