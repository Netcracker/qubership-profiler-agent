package com.netcracker.profiler.instrument.custom.impl;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.agent.EnhancementRegistry;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.util.List;

public class Weblogic81ServletInvocationAction_run extends HttpServletRequest_run {
    public static final Logger log = LoggerFactory.getLogger(Weblogic81ServletInvocationAction_run.class);

    private final static Type c_ServletInvocationAction = Type.getObjectType("weblogic/servlet/internal/WebAppServletContext$ServletInvocationAction");
    private final static Type c_ServletRequestImpl = Type.getObjectType("weblogic/servlet/internal/ServletRequestImpl");

    @Override
    protected void loadRequestObject(ProfileMethodAdapter ma) {
        ma.loadSavedThis();
        ma.getField(c_ServletInvocationAction, "req", c_ServletRequestImpl);
    }

    @Override
    protected Type getTargetClass() {
        return c_ServletInvocationAction;
    }

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        final EnhancementRegistry er = configuration.getEnhancementRegistry();
        final List def = er.getEnhancers("com/netcracker/profiler/instrument/enhancement/WebLogicHTTPEnhancer");
        if (def.isEmpty()) {
            log.warn("Looks like http enhancer is not loaded. The profiling of {} would not be performed", getTargetClass().getClassName());
        } else {
            er.addEnhancer(getTargetClass().getInternalName(), def.get(0));
        }
        return this;
    }
}
