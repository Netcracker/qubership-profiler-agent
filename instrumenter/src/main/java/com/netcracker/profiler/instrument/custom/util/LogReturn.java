package com.netcracker.profiler.instrument.custom.util;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class LogReturn extends MethodInstrumenter {
    private final static Logger log = LoggerFactory.getLogger(LogReturn.class);
    private String eventName;
    private Boolean earlyToString;

    @Override
    public LogReturn init(Element e, Configuration_01 configuration) {
        String eventName = e.getAttribute("name");
        if (eventName.length() == 0) {
            eventName = "result";
            log.warn("Event name is not specified, using {}", eventName);
        }
        this.eventName = eventName;

        String earlyToString = e.getAttribute("early-to-string");
        if (earlyToString.length() != 0)
            this.earlyToString = Boolean.valueOf(earlyToString);
        return this;
    }

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        final Type type = ma.getReturnType();
        if (Type.VOID_TYPE.equals(type)) {
            log.warn("Unable to log void return value from method {}", ma.getMethodFullName());
            return;
        }
        if (type.getSize() == 1)
            ma.dup();
        else
            ma.dup2();
        ma.box(type);
        ma.logEvent(eventName, type, earlyToString == null ? false : earlyToString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogReturn)) return false;
        if (!super.equals(o)) return false;

        LogReturn that = (LogReturn) o;

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
