package com.netcracker.profiler.fetch;

import com.netcracker.profiler.analyzer.AggregateJFRStacks;
import com.netcracker.profiler.analyzer.MergeTrees;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.stack.DumpVisitor;
import com.netcracker.profiler.sax.stack.DumpsVisitor;
import com.netcracker.profiler.threaddump.parser.MethodThreadLineInfo;
import com.netcracker.profiler.threaddump.parser.ThreadInfo;
import com.netcracker.profiler.threaddump.parser.ThreaddumpParser;
import com.netcracker.profiler.util.ProfilerConstants;

import org.openjdk.jmc.common.IMCFrame;
import org.openjdk.jmc.common.IMCMethod;
import org.openjdk.jmc.common.IMCStackTrace;
import org.openjdk.jmc.common.IMCType;
import org.openjdk.jmc.common.item.*;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.flightrecorder.JfrAttributes;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes;

import java.io.File;

public abstract class FetchJFRDump implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final String jfrFileName;
    protected IMemberAccessor<IMCStackTrace, IItem> stacktraceAccessor;
    protected IMemberAccessor<IQuantity, IItem> threadId;

    public FetchJFRDump(ProfiledTreeStreamVisitor sv, String jfrFileName) {
        this.sv = sv;
        this.jfrFileName = jfrFileName;
    }

    protected abstract IItemFilter getIItemFilter();

    protected void onNextItemType(IType<IItem> itemType) {
        stacktraceAccessor = JfrAttributes.EVENT_STACKTRACE.getAccessor(itemType);
        threadId = JdkAttributes.EVENT_THREAD_ID.getAccessor(itemType);
    }

    public void run() {
        DumpsVisitor processChain = getDumpsProcessChain(sv);

        try {
            DumpVisitor dump = processChain.visitDump();
            File jfrFile = new File(jfrFileName);
            IItemCollection eventTypes = JfrLoaderToolkit.loadEvents(jfrFile);
            IItemCollection filteredEvents = eventTypes.apply(getIItemFilter());
            for (IItemIterable items : filteredEvents) {
                onNextItemType(items.getType());
                for (IItem item : items) {
                    ThreadInfo thread = parseThread(item);
                    dump.visitThread(thread);
                }
            }
            dump.visitEnd();
        } catch (Throwable e) {
            ErrorSupervisor.getInstance().warn(
                    "Error processing " + jfrFileName, e);
        } finally {
            processChain.visitEnd();
        }

    }

    protected ThreadInfo parseThread(IItem event) {
        ThreadInfo threadinfo = new ThreadInfo();

        threadinfo.daemon = true;
        threadinfo.priority = "10";
        IQuantity thread = threadId.getMember(event);
        threadinfo.threadID = thread == null ? null : String.valueOf(thread.longValue());
        threadinfo.state = "ACTIVE";
        threadinfo.value = 1;

        addStackTrace(event, threadinfo);

        return threadinfo;
    }

    protected void addStackTrace(IItem event, ThreadInfo threadinfo) {

        IMCStackTrace trace = stacktraceAccessor.getMember(event);
        if (trace == null || trace.getFrames() == null) {
            threadinfo.addThreadLine(parseThreadLine(null));
            return;
        }
        for (IMCFrame frame : trace.getFrames()) {
            threadinfo.addThreadLine(parseThreadLine(frame));
        }
    }

    protected String getFriendlyClassName(String className) {
        int arrayDepth = 0;
        for (; arrayDepth < className.length(); arrayDepth++) {
            if (className.charAt(arrayDepth) != '[') {
                break;
            }
        }
        StringBuilder sb = new StringBuilder(className.length() + 5 + arrayDepth * 2);
        className = className.replace('/', '.');
        if (arrayDepth == 0) {
            sb.append(className);
        } else if (className.charAt(arrayDepth) == 'L') {
            sb.append(className, arrayDepth + 1, className.length() - 1);
        } else {
            sb.append("java.primitive.");
            switch (className.charAt(arrayDepth)) {
                case 'Z':
                    sb.append("boolean");
                    break;
                case 'B':
                    sb.append("byte");
                    break;
                case 'S':
                    sb.append("short");
                    break;
                case 'C':
                    sb.append("char");
                    break;
                case 'I':
                    sb.append("int");
                    break;
                case 'J':
                    sb.append("long");
                    break;
                case 'D':
                    sb.append("double");
                    break;
                case 'F':
                    sb.append("float");
                    break;
                default:
                    sb.append(className.charAt(arrayDepth));
            }
        }

        for (; arrayDepth > 0; arrayDepth--) {
            sb.append("[]");
        }
        return sb.toString();
    }

    protected ThreaddumpParser.ThreadLineInfo parseThreadLine(IMCFrame frame) {
        IMCMethod frameMethod = frame == null ? null : frame.getMethod();
        if (frameMethod != null) {
            IMCType classRef = frameMethod.getType();
            String className = classRef.getTypeName();
            if(className != null) {
                MethodThreadLineInfo method = new MethodThreadLineInfo();
                method.setClassName(classRef.getFullName());
                method.methodName = frameMethod.getMethodName();
                method.locationClass = className;
                method.locationLineNo = frame.getFrameLineNumber() + ", bci " + frame.getBCI();
                return method;
            }
        }
        MethodThreadLineInfo method = new MethodThreadLineInfo();

        method.setClassName("com.java.Unknown");
        method.methodName = "unknown";
        method.locationClass = "com.java.Unknown";
        method.locationLineNo = "unknown";
        return method;
    }

    private DumpsVisitor getDumpsProcessChain(ProfiledTreeStreamVisitor sv) {
        ProfiledTreeStreamVisitor merge = new MergeTrees(sv);
        DumpsVisitor agg = new AggregateJFRStacks(merge);
        return new DumpsVisitor(ProfilerConstants.PROFILER_V1, agg) {
            @Override
            public DumpVisitor visitDump() {
                DumpVisitor out = super.visitDump();
                //out = new FilterThreadStacks(out);
                //out = new MoveLockLineUp(out);
                return out;
            }
        };
    }
}
