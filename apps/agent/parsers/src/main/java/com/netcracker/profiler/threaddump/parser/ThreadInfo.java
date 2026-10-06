package com.netcracker.profiler.threaddump.parser;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Contains information on single thread in the dump
 */
public class ThreadInfo implements ThreaddumpParser.JSerializable {
    public String name, priority, threadID, state;
    public String obj;
    public long value;

    public boolean daemon = false;
    public ArrayList<ThreaddumpParser.ThreadLineInfo> stackTrace = new ArrayList<ThreaddumpParser.ThreadLineInfo>();

    public StringBuffer toJS(StringBuffer sb) {
        sb.append("new ThreadInfo('");
        sb.append(ThreaddumpParser.toJString(state)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(name)).append(ThreaddumpParser.SEP);
        sb.append(daemon ? '1' : '0').append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(priority)).append(ThreaddumpParser.SEP);
        sb.append(ThreaddumpParser.toJString(threadID)).append(ThreaddumpParser.SEP);
        sb.append(value).append(ThreaddumpParser.SEP);
        sb.append("', [");
        for (Iterator<ThreaddumpParser.ThreadLineInfo> it = stackTrace.iterator(); it.hasNext(); ) {
            (it.next()).toJS(sb);
            sb.append(", ");
        }
        return sb.append("])");
    }

    public void addThreadLine(ThreaddumpParser.ThreadLineInfo line) {
        if (line != null)
            stackTrace.add(line);
    }
}
