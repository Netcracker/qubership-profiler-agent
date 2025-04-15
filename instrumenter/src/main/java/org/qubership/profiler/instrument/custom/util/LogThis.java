package org.qubership.profiler.instrument.custom.util;

import org.qubership.profiler.agent.Configuration_01;
import org.qubership.profiler.instrument.ProfileMethodAdapter;
import org.qubership.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class LogThis extends MethodInstrumenter {
    private final static Logger log = LoggerFactory.getLogger(LogThis.class);

    private String eventName;
    private Boolean earlyToString;

    @Override
    public LogThis init(Element e, Configuration_01 configuration) {
        String eventName = e.getAttribute("name");
        if (eventName.length() == 0) {
            eventName = "this";
            log.warn("Event name is not specified, using {}", eventName);
        }
        this.eventName = eventName;

        String earlyToString = e.getAttribute("early-to-string");
        if (earlyToString.length() != 0)
            this.earlyToString = Boolean.valueOf(earlyToString);
        return this;
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        ma.declareThisVariable();
    }

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {
        ma.loadSavedThis();
        Type type = Type.getType(Object.class);
        ma.box(type);
        ma.logEvent(eventName, type, earlyToString == null ? false : earlyToString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogThis)) return false;
        if (!super.equals(o)) return false;

        LogThis that = (LogThis) o;

        if (eventName != null ? !eventName.equals(that.eventName) : that.eventName != null) return false;
        if (earlyToString != null ? !earlyToString.equals(that.earlyToString) : that.earlyToString != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (eventName != null ? eventName.hashCode() : 0);
        result = 31 * result + (earlyToString != null ? earlyToString.hashCode() : 0);
        return result;
    }
}
