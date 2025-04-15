package org.qubership.profiler.instrument.custom.impl;

import org.qubership.profiler.agent.Configuration;
import org.qubership.profiler.instrument.custom.ClassAcceptor;

import org.w3c.dom.Element;

public abstract class ClassInstrumenter implements ClassAcceptor {
    public ClassInstrumenter init(Element e, Configuration configuration) {
        return this;
    }
}
