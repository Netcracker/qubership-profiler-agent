package org.qubership.profiler.analyzer;

import org.qubership.profiler.sax.stack.DumpVisitor;
import org.qubership.profiler.threaddump.parser.LockThreadLineInfo;
import org.qubership.profiler.threaddump.parser.ThreadInfo;
import org.qubership.profiler.threaddump.parser.ThreaddumpParser;
import org.qubership.profiler.util.ProfilerConstants;

import java.util.ArrayList;

public class FilterThreadStacks extends DumpVisitor {
    public FilterThreadStacks(DumpVisitor dv) {
        super(ProfilerConstants.PROFILER_V1, dv);
    }

    protected FilterThreadStacks(int api, DumpVisitor dv) {
        super(api, dv);
    }

    @Override
    public void visitThread(ThreadInfo thread) {
        final ArrayList<ThreaddumpParser.ThreadLineInfo> trace = thread.stackTrace;
        if (trace.size() == 0)
            return;

        final ThreaddumpParser.ThreadLineInfo firstLine = trace.get(0);

        boolean isObjectWait = firstLine.isMethodLine("java.lang.Object", "wait");

        if (trace.size() <= 7 && isObjectWait
                || trace.size() <= 7 && firstLine.isMethodLine("java.lang.Thread", "sleep")
                || firstLine.isMethodLine("jrockit.vm.Locks", "park0") && (
                trace.size() <= 9 && (
                       trace.get(7).isMethodLine("java.util.concurrent.ThreadPoolExecutor", "getTask")
                                ||trace.get(8).isMethodLine("java.util.concurrent.ThreadPoolExecutor", "getTask")
                )
        )
                )
            return; // assuming idle thread

        if (isObjectWait && trace.get(trace.size() - 1).isMethodLine("org.qubership.ejb.cluster.ClusterThread", "run")) {
            return; // idle cluster service
        }

        if (firstLine instanceof LockThreadLineInfo && trace.size() > 1) {
            ThreaddumpParser.ThreadLineInfo secondLine = trace.get(1);
            boolean isPark = secondLine.isMethodLine("sun.misc.Unsafe", "park");
            if (trace.size() <= 9 && isPark
                    || trace.size() <= 7 &&secondLine.isMethodLine("java.lang.Object", "wait")
                    || trace.size() <= 9 &&secondLine.isMethodLine("jrockit.vm.Threads", "waitForNotifySignal")
                    || trace.size() > 5 && trace.size() < 9 &&trace.get(4).isMethodLine("com.ooc.OB.ThreadPool", "get")
                    )
                return; // assuming idle thread
            if (trace.size() > 7
                    && (trace.get(6).isMethodLine("java.util.concurrent.ThreadPoolExecutor", "getTask")
                    || trace.get(7).isMethodLine("java.util.concurrent.ThreadPoolExecutor", "getTask")
                    || trace.get(5).isMethodLine("java.util.concurrent.ThreadPoolExecutor", "getTask")
            ))
                return; // Idle executor thread

            if (isPark && trace.size() > 7
                    && (trace.get(4).isMethodLine("java.util.concurrent.ArrayBlockingQueue", "take")
                    && trace.get(5).isMethodLine("org.qubership.framework.tasktracking.QueueTrackingService$QueueTrackingTask", "run")
                    || trace.get(6).isMethodLine("org.qubership.mediation.common.clustering.impl.NCClusterHelper", "run")
                    || trace.get(5).isMethodLine("java.util.concurrent.LinkedTransferQueue", "take")
                    && trace.get(6).isMethodLine("org.qubership.mediation.common.clustering.impl.NCClusterHelper", "run")
            ))
                return; // idle thread
        }

        if (thread.name == null)
            return; // ignore invalid records

        if (thread.name.contains("weblogic.socket.Muxer") ||
                thread.name.contains("weblogic.cluster.MessageReceiver") ||
                thread.name.startsWith("DynamicListenThread") ||
                thread.name.equals("main")
                ) return; // we do not need weblogic system threads

        if (thread.name.startsWith("RMI TCP") &&
                (firstLine.isMethodLine("java.net.PlainSocketImpl", "socketAccept") ||
                        firstLine.isMethodLine("jrockit.net.SocketNativeIO", "readBytesPinned")
                ))
            return;

        if ((trace.size() == 9 || trace.size() == 10) &&
               trace.get(trace.size() - 4).isMethodLine("weblogic.server.channels.DynamicListenThread$SocketAccepter", "accept") &&
                firstLine.isMethodLine("java.net.PlainSocketImpl", "socketAccept"))
            return;

        if ((thread.name.startsWith("RcvThread") ||
                thread.name.startsWith("LDAPConnThread-") ||
                thread.name.startsWith("ActiveMQ Transport")) && firstLine.isMethodLine("java.net.SocketInputStream", "socketRead0"))
            return;

        if (thread.name.startsWith("DispatchThread") && isObjectWait)
            return;

        if (thread.name.startsWith("UnknownHub Hub") || thread.name.startsWith("Temp Instrumentation") || thread.name.startsWith("Agent"))
            return;

        if (trace.size() == 11 &&firstLine.isLockLine("weblogic.platform.SunVM"))
            return;

        if (trace.size() == 18 &&trace.get(14).isLockLine("weblogic.rjvm.http.HTTPClientJVMConnection"))
            return;

        if (trace.size() > 10 &&trace.get(4).isMethodLine("weblogic.t3.srvr.JVMRuntime", "getThreadStackDump"))
            return;

        if (trace.size() == 9 && firstLine.isMethodLine("java.net.SocketInputStream", "socketRead0") &&
               trace.get(trace.size() - 1).isMethodLine("com.tibco.tibjms.TibjmsxLinkTcp$LinkReader", "run"))
            return;

        if (trace.size() == 13 &&trace.get(2).isMethodLine("com.tibco.tibjms.TibjmsxSessionImp", "_getSyncMessage"))
            return;

        if (thread.name.startsWith("Dispatcher") && isObjectWait &&
               trace.get(trace.size() - 1).isMethodLine("com.ibm.msg.client.commonservices.j2se.workqueue.WorkQueueManagerImplementation$ThreadPoolWorker", "run"))
            return;

        if (trace.size() <= 9 && firstLine.isMethodLine("sun.misc.Unsafe", "park"))
            return; // assuming idle thread

        if (trace.size() > 7 && trace.size() <= 13 &&trace.get(7).isMethodLine("sun.nio.ch.SelectorImpl", "select"))
            return; // NIO selector thread

        if (trace.size() <= 10 && firstLine.isMethodLine("sun.nio.fs.LinuxWatchService", "poll"))
            return; // watch service idle thread

        if (trace.size() == 3 && firstLine.isMethodLine("org.apache.tomcat.jni.Socket", "accept"))
            return; // Idle Tomcat acceptor

        if (trace.size() == 3 && firstLine.isMethodLine("org.apache.tomcat.jni.Poll", "poll"))
            return; // Idle Tomcat acceptor

        if (firstLine.isMethodLine("sun.nio.ch.EPollArrayWrapper", "epollWait"))
            return; // assuming idle thread

        super.visitThread(thread);
    }
}
