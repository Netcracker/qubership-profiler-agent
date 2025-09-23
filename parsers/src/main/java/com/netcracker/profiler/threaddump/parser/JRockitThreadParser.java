package com.netcracker.profiler.threaddump.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for JRockit thread dumps
 */
public class JRockitThreadParser implements ThreadFormatParser {
    private final static Logger log = LoggerFactory.getLogger(JRockitThreadParser.class);
    public ThreadInfo parseThread(String s) {
        ThreadInfo threadinfo = new ThreadInfo();
        Matcher matcher = threadPattern.matcher(s);
        if (matcher.lookingAt()) {
            threadinfo.name = matcher.group(1);
            String s1 = matcher.group(5);
            threadinfo.daemon = s1 != null;
            threadinfo.priority = matcher.group(3);
            threadinfo.threadID = matcher.group(2);
            threadinfo.state = matcher.group(4).trim();
        } else {
            log.error("parseThread failed on: '" + s + "' using pattern '" + threadPattern + "'");
        }
        return threadinfo;
    }

    public ThreaddumpParser.ThreadLineInfo parseThreadLine(String s) {
        if ("    -- end of trace".equals(s))
            return null;
        Matcher matcher = methodPattern.matcher(s);
        ThreaddumpParser.ThreadLineInfo threadLineInfo;
        if (matcher.lookingAt()) {
            MethodThreadLineInfo method = new MethodThreadLineInfo();
            String s1 = matcher.group(1);
            int i = s1.lastIndexOf('.');
            method.setClassName(s1.substring(0, i).replace('/', '.'));
            method.methodName = s1.substring(i + 1);
            if (matcher.group(5) != null)
                method.methodName += ' ' + matcher.group(5);
            String s2 = matcher.group(4);
            i = s2.indexOf(':');
            if (i == -1) {
                if (s2.length() == 0)
                    method.locationClass = "Unknown";
                else
                    method.locationClass = s2;
            } else {
                method.locationClass = s2.substring(0, i);
                method.locationLineNo = s2.substring(i + 1);
            }
            method.arguments = matcher.group(2);
            if (method.arguments != null)
                method.arguments = method.arguments.replace('/', '.');
            method.returnValue = matcher.group(3);
            if (method.returnValue != null)
                method.returnValue = method.returnValue.replace('/', '.');

            threadLineInfo = method;
        } else if ((matcher = lockPattern.matcher(s)).lookingAt()) {
            LockThreadLineInfo lock = new LockThreadLineInfo();
            lock.type = matcher.group(1).intern();
            lock.id = matcher.group(3);
            lock.className = matcher.group(2).replace('/', '.') + ' ' + matcher.group(4);
            threadLineInfo = lock;
        } else {
            if ("    -- Blocked trying to get lock on an unknown object".equals(s)) {
                LockThreadLineInfo lock = new LockThreadLineInfo();
                lock.type = "Blocked trying to get lock";
                lock.className = "UnknownObject";
                return lock;
            }
            log.error("Unknown line: '" + s + "'");
            threadLineInfo = new MethodThreadLineInfo();
        }
        return threadLineInfo;
    }

    private final Pattern threadPattern = Pattern.compile("\"(.*?)\" id=(\\S+) idx=\\S+ tid=\\S+ prio=(\\S+) (\\S+)(?:, [^,]+)*(, daemon)?");
    private final Pattern methodPattern = Pattern.compile("    at ([\\p{Alnum}$_.<>/]+)(?:\\(([^)]*)\\)([^(]+))?\\(([^);/]+(?::\\d+)?|Native Method|Unknown Source)\\)(\\[(?:optimized|inlined)\\])?");
    private final Pattern lockPattern = Pattern.compile("    \\^?-- (Waiting for notification on|Lock released while waiting|Holding lock|Blocked trying to get lock|Parking to wait for): ([^@]+)@([^\\[]+)(\\[(?:fat lock|thin lock|biased lock|recursive|unlocked)\\])?");
}
