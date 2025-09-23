package com.netcracker.profiler.instrument.custom.util;

import com.netcracker.profiler.agent.Configuration_01;
import com.netcracker.profiler.agent.StringUtils;
import com.netcracker.profiler.instrument.ProfileMethodAdapter;
import com.netcracker.profiler.instrument.custom.MethodAcceptor;
import com.netcracker.profiler.instrument.custom.MethodInstrumenter;

import org.w3c.dom.Element;

import java.util.Objects;

public class LogException extends MethodInstrumenter {
    private MethodAcceptor dumpExceptionDelegate;
    private MethodAcceptor logExceptionDelegate;
    private MethodAcceptor callRedDelegate;

    @Override
    public MethodInstrumenter init(Element e, Configuration_01 configuration) {
        boolean self = Boolean.valueOf(e.getAttribute("self"));
        String executeMethodSuffix;
        if(self) {
            executeMethodSuffix = "(java.lang.Throwable this)";
        } else {
            executeMethodSuffix = "";
            e.setAttribute("exception-only", "true");
        }

        e.setAttribute("static", "true");
        e.setAttribute("class", "com.netcracker.profiler.agent.ExceptionLogger");

        String dump = e.getAttribute("dump");
        String log = e.getAttribute("log");

        if(!StringUtils.isBlank(dump)) {
            switch (dump) {
                default :
                case "class" :
                    e.setTextContent("dumpExceptionClass" + executeMethodSuffix);
                    break;
                case "message" :
                    e.setTextContent("dumpExceptionWithMessage" + executeMethodSuffix);
                    break;
                case "stacktrace" :
                    e.setTextContent("dumpExceptionWithMessageAndStacktrace" + executeMethodSuffix);
                    break;
            }
            dumpExceptionDelegate = newDelegate(e, configuration, self);
        }

        if(!StringUtils.isBlank(log)) {
            switch (log) {
                default :
                case "class" :
                    e.setTextContent("logExceptionClass" + executeMethodSuffix);
                    break;
                case "message" :
                    e.setTextContent("logExceptionWithMessage" + executeMethodSuffix);
                    break;
                case "stacktrace" :
                    e.setTextContent("logExceptionWithMessageAndStacktrace" + executeMethodSuffix);
                    break;
            }
            logExceptionDelegate = newDelegate(e, configuration, self);
        }

        boolean callRed = Boolean.valueOf(e.getAttribute("call-red"));
        if(callRed) {
            e.setTextContent("callRed" + executeMethodSuffix);
            callRedDelegate = newDelegate(e, configuration, self);
        }

        return super.init(e, configuration);
    }

    private MethodAcceptor newDelegate(Element e, Configuration_01 configuration, boolean self) {
        MethodAcceptor methodAcceptor = new ExecuteMethodAfter().init(e, configuration);
        if(!self) {
            methodAcceptor = new GuardedAction(methodAcceptor).init(e, configuration);
        }
        return methodAcceptor;
    }

    @Override
    public void declareLocals(ProfileMethodAdapter ma) {
        if(dumpExceptionDelegate != null) dumpExceptionDelegate.declareLocals(ma);
        if(logExceptionDelegate != null) logExceptionDelegate.declareLocals(ma);
        if(callRedDelegate != null) callRedDelegate.declareLocals(ma);
    }

    @Override
    public void onMethodExit(ProfileMethodAdapter ma) {
        if(dumpExceptionDelegate != null) dumpExceptionDelegate.onMethodExit(ma);
        if(logExceptionDelegate != null) logExceptionDelegate.onMethodExit(ma);
        if(callRedDelegate != null) callRedDelegate.onMethodExit(ma);
    }

    @Override
    public void onMethodException(ProfileMethodAdapter ma) {
        if(dumpExceptionDelegate != null) dumpExceptionDelegate.onMethodException(ma);
        if(logExceptionDelegate != null) logExceptionDelegate.onMethodException(ma);
        if(callRedDelegate != null) callRedDelegate.onMethodException(ma);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogException)) return false;
        if (!super.equals(o)) return false;

        LogException that = (LogException) o;
        if (!Objects.equals(dumpExceptionDelegate, that.dumpExceptionDelegate)) return false;
        if (!Objects.equals(logExceptionDelegate, that.logExceptionDelegate)) return false;
        if (!Objects.equals(callRedDelegate, that.callRedDelegate)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (dumpExceptionDelegate != null ? dumpExceptionDelegate.hashCode() : 0);
        result = 31 * result + (logExceptionDelegate != null ? logExceptionDelegate.hashCode() : 0);
        result = 31 * result + (callRedDelegate != null ? callRedDelegate.hashCode() : 0);
        return result;
    }

}
