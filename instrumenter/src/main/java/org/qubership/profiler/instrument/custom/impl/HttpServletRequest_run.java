package org.qubership.profiler.instrument.custom.impl;

import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.agent.EnhancementRegistry;
import org.qubership.profiler.instrument.ProfileMethodAdapter;
import org.qubership.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.List;

public abstract class HttpServletRequest_run extends MethodInstrumenter implements Opcodes {
    public static final Logger log = LoggerFactory.getLogger(HttpServletRequest_run.class);

    protected final static Method m_dumpRequest = Method.getMethod("void dumpRequest$profiler(javax.servlet.ServletRequest)");
    protected final static Method m_afterRequest = Method.getMethod("void afterRequest$profiler(javax.servlet.ServletRequest)");

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        final EnhancementRegistry er = configuration.getEnhancementRegistry();
        final List def = er.getEnhancers("org/qubership/profiler/instrument/enhancement/TomcatHTTPEnhancer");
        if (def.isEmpty()) {
            log.warn("Looks like http enhancer is not loaded. The profiling of {} would not be performed", getTargetClass().getClassName());
        } else {
            er.addEnhancer(getTargetClass().getInternalName(), def.get(0));
        }
        return this;
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        ma.declareThisVariable();
    }

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {
        loadRequestObject(ma);
        ma.invokeStatic(getTargetClass(), m_dumpRequest);
    }

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        loadRequestObject(ma);
        ma.invokeStatic(getTargetClass(), m_afterRequest);
    }

    @Override
    public void onMethodException(ProfileMethodAdapter ma) {
        onMethodExit(ma);
    }

    protected abstract void loadRequestObject(ProfileMethodAdapter ma);

    protected abstract Type getTargetClass();
}
