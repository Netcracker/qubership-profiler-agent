package com.netcracker.profiler.instrument.custom.util;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;
import com.netcracker.profiler.util.XMLHelper;

import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class LogParameter extends MethodInstrumenter {
    private final static Logger log = LoggerFactory.getLogger(LogParameter.class);

    private String eventName;
    private int parameterIndex;
    private int savedParameterIndex;
    private Boolean earlyToString;

    @Override
    public LogParameter init(Element e, Configuration_01 configuration) {
        String content = XMLHelper.getTextContent(e);
        int parameter = -1;
        try {
            parameter = Integer.parseInt(content);
        } catch (NumberFormatException e1) {
             /**/
        }
        if (parameter == -1) {
            parameter = 0;
            log.warn("Detected unparseable log-parameter record. log-parameter tag should contain an integer. Assuming the first one");
        }
        parameterIndex = parameter;

        String eventName = e.getAttribute("name");
        if (eventName.length() == 0) {
            eventName = "p" + parameter;
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
        savedParameterIndex = ma.saveArg(parameterIndex);
    }

    @Override
    public void onMethodEnter(ProfileMethodAdapter ma) {
        Type[] argumentTypes = ma.getArgumentTypes();
        if (parameterIndex >= argumentTypes.length) {
            log.warn("Unable to log parameter {} as {} in method {} since method has only {} parameters", new Object[]{parameterIndex, eventName, ma.getMethodFullName(), argumentTypes.length});
            return;
        }
        log.debug("Logging parameter {} as {} in method {}", new Object[]{parameterIndex, eventName, ma.getMethodFullName()});
        ma.loadLocal(savedParameterIndex);
        Type argumentType = argumentTypes[parameterIndex];
        ma.box(argumentType);
        ma.logEvent(eventName, argumentType, earlyToString == null ? false : earlyToString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogParameter)) return false;
        if (!super.equals(o)) return false;

        LogParameter that = (LogParameter) o;

        if (parameterIndex != that.parameterIndex) return false;
        if (eventName != null ? !eventName.equals(that.eventName) : that.eventName != null) return false;
        if (earlyToString != null ? !earlyToString.equals(that.earlyToString) : that.earlyToString != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (eventName != null ? eventName.hashCode() : 0);
        result = 31 * result + parameterIndex;
        result = 31 * result + (earlyToString != null ? earlyToString.hashCode() : 0);
        return result;
    }
}
