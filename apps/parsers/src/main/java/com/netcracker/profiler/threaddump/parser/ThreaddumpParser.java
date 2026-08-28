package com.netcracker.profiler.threaddump.parser;

import java.util.*;

public class ThreaddumpParser {
    static final String SEP = "\\000";

    static String toJString(String x) {
        if (x == null) return "";
        if (x.indexOf('\\') != -1) x = x.replaceAll("\\\\", "\\\\\\\\");
        if (x.indexOf('\'') != -1) x = x.replaceAll("'", "\\\\'");
        if (x.indexOf('\n') != -1) x = x.replaceAll("\n", "\\\\n");
        if (x.indexOf('\r') != -1) x = x.replaceAll("\r", "\\\\r");
        if (x.indexOf('\r') != -1) x = x.replaceAll("\r", "\\\\r");
        x = x.replace('\0', '\1');
        return x;
    }

    public interface JSerializable {
        StringBuffer toJS(StringBuffer sb);
    }

    public interface ThreadLineInfo extends JSerializable {
        boolean isMethodLine(String className, String methodName);
        boolean isLockLine(String className);

    }

    interface ThreadDump extends JSerializable {
        void addException(Exception e);

        Collection<? extends Object> getExceptions();

        void setThreadFormatParser(ThreadFormatParser threadformatparser);

        void setPreDumpInfo(String s);

        void setPostDumpInfo(String s);

        Collection<? extends Object>/*<ThreadInfo>*/ getThreads();

        void addThread(String s);

        void addThreadLine(String s);

        void addDeadLockInfo(String s);

        void addLocks(LinkedList<Object> linkedlist);

        void setRaw(String raw);

        String getRaw();

        public void setServer(String srv, String os, String arch);

        void setTitle(String title);

        String getTitle();
    }


    static class ThreadDumpImpl implements ThreadDump {

        public void setTitle(String title) {
            this.dumpTitle = title;
        }

        public String getTitle() {
            return dumpTitle;
        }

        public void setServer(String srv, String os, String arch) {
            this.srv = srv;
            this.os = os;
            this.arch = arch;
        }

        public void setThreadFormatParser(ThreadFormatParser threadformatparser) {
            parser = threadformatparser;
        }

        public void setPreDumpInfo(String s) {
            preDumpInfo = s;
        }

        public void setPostDumpInfo(String s) {
            postDumpInfo = s;
        }

        public void addThread(String s) {
            currentThread = parser.parseThread(s);
            threads.add(currentThread);
        }

        public void addThreadLine(String s) {
            ThreadLineInfo threadlineinfo = parser.parseThreadLine(s);
            if (threadlineinfo != null)
                currentThread.addThreadLine(threadlineinfo);
        }

        public void addDeadLockInfo(String s) {
            deadlockInfo = s;
        }

        public void addLocks(LinkedList<Object> linkedlist) {
            standaloneLocks.addAll(linkedlist);
        }


        public Collection<ThreadInfo> getThreads() {
            return threads;
        }

        public void addException(Exception e) {
            exceptions.add(e);
        }

        public Collection getExceptions() {
            return exceptions;
        }

        public void setRaw(String raw) {
            this.raw = raw;
        }

        public String getRaw() {
            return raw;
        }

        public StringBuffer toJS(StringBuffer sb) {
            sb.append("new ThreadDump('");
            sb.append(toJString(srv)).append("', '");
            sb.append(toJString(os)).append("', '");
            sb.append(toJString(arch)).append("', '");
            sb.append(toJString(raw));
            sb.append("'\n, [");
            for (Iterator it = threads.iterator(); it.hasNext(); ) {
                ((JSerializable) it.next()).toJS(sb);
                sb.append(",\n");
            }
            return sb.append("])\n");
        }

        String timeBefore, timeAfter;
        List<ThreadInfo> threads = new ArrayList<ThreadInfo>();
        ThreadInfo currentThread;
        String dumpTitle, deadlockInfo;
        String preDumpInfo, postDumpInfo;
        ThreadFormatParser parser;
        LinkedList<Object> standaloneLocks = new LinkedList<Object>();
        Collection<Exception> exceptions = new ArrayList<Exception>();
        String raw, srv, os, arch;
    }


}
