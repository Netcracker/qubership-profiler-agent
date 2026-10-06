package org.mozilla.javascript.regexp;

import com.netcracker.profiler.agent.Profiler;

import org.mozilla.javascript.ScriptRuntime;
import org.mozilla.javascript.Scriptable;

public class RegExpImpl {
    private void dumpRegexp$profiler(Scriptable thisObj, Object[] args) {
        dumpArgs$profiler(thisObj, "input.string");
        if (args == null || args.length==0)
            return;
        dumpArgs$profiler(args[0], "regexp");
    }

    private void dumpArgs$profiler(Object arg, String name) {
        String value = null;
        try {
            value = ScriptRuntime.toString(arg);
        } catch(Throwable t) {
            value = "Unable to convert value to string: " + t.getMessage();
        }
        if (value != null)
            Profiler.event(value, name);
    }
}
