package org.qubership.profiler.analyzer;

import org.qubership.profiler.sax.stack.DumpVisitor;
import org.qubership.profiler.threaddump.parser.LockThreadLineInfo;
import org.qubership.profiler.threaddump.parser.ThreadInfo;
import org.qubership.profiler.threaddump.parser.ThreaddumpParser;
import org.qubership.profiler.util.ProfilerConstants;

import java.util.ArrayList;

public class MoveLockLineUp extends DumpVisitor {
    public MoveLockLineUp(DumpVisitor dv) {
        this(ProfilerConstants.PROFILER_V1, dv);
    }

    protected MoveLockLineUp(int api, DumpVisitor dv) {
        super(api, dv);
    }

    @Override
    public void visitThread(ThreadInfo thread) {
        final ArrayList<ThreaddumpParser.ThreadLineInfo> st = thread.stackTrace;
        if (st.size() <= 1) return;
        final ThreaddumpParser.ThreadLineInfo second = st.get(1);
        if (!(second instanceof LockThreadLineInfo)) {
            super.visitThread(thread);
            return;
        }

        LockThreadLineInfo lock = (LockThreadLineInfo) second;
        final String type = lock.type;
        if (LockThreadLineInfo.TYPE_ENTRY.equals(type) ||
                LockThreadLineInfo.TYPE_WAIT.equals(type) ||
                LockThreadLineInfo.TYPE_PARKING.equals(type)) {
            st.set(1, st.get(0));
            st.set(0, second);
        }

        super.visitThread(thread);
    }
}
