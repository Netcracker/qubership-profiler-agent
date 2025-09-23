package com.netcracker.profiler.instrument.custom.impl;

import com.netcracker.profiler.agent.Configuration;
import com.netcracker.profiler.instrument.custom.ClassAcceptor;

import org.w3c.dom.Element;

public abstract class ClassInstrumenter implements ClassAcceptor {
    public ClassInstrumenter init(Element e, Configuration configuration) {
        return this;
    }
}
