package com.netcracker.profiler.instrument.custom.impl;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.agent.EnhancementRegistry;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.List;

public class Jetty10RequestHandler extends HttpServletRequest_run {
    public static final Logger log = LoggerFactory.getLogger(Jetty10RequestHandler.class);

    private final static Type c_ServletHandler = Type.getObjectType("org/eclipse/jetty/servlet/ServletHandler");

    protected final static Method m_dumpRequest = Method.getMethod("void dumpRequest$profiler(jakarta.servlet.ServletRequest)");
    protected final static Method m_afterRequest = Method.getMethod("void afterRequest$profiler(jakarta.servlet.ServletRequest)");


    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        final EnhancementRegistry er = configuration.getEnhancementRegistry();
        final List def = er.getEnhancers("com/netcracker/profiler/instrument/enhancement/Jetty10HTTPEnhancer");
        if (def.isEmpty()) {
            log.warn("Looks like http enhancer is not loaded. The profiling of {} would not be performed", getTargetClass().getClassName());
        } else {
            er.addEnhancer(getTargetClass().getInternalName(), def.get(0));
        }
        return this;
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        ma.saveArg(2);
    }

    @Override
    public void loadRequestObject(ProfileMethodAdapter ma) {
        ma.loadLocal(ma.saveArg(2));
    }

    @Override
    protected Type getTargetClass() {
        return c_ServletHandler;
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
}
