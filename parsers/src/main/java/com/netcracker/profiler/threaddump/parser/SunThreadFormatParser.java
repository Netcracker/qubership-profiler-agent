package com.netcracker.profiler.threaddump.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Sun JDK thread dumps
 */
public class SunThreadFormatParser implements ThreadFormatParser {
    private static final Logger log = LoggerFactory.getLogger(SunThreadFormatParser.class);
    public ThreadInfo parseThread(String s) {
        ThreadInfo threadinfo = new ThreadInfo();
        Matcher matcher = threadPattern.matcher(s);
        if (matcher.lookingAt()) {
            threadinfo.name = matcher.group(1);
            String s1 = matcher.group(2);
            threadinfo.daemon = s1 != null;
            threadinfo.priority = matcher.group(3);
            threadinfo.threadID = matcher.group(4);
            threadinfo.state = matcher.group(5).trim();
        } else {
            log.error("parseThread failed on: '" + s + "' using pattern '" + threadPattern + "'");
        }
        return threadinfo;
    }

    public ThreaddumpParser.ThreadLineInfo parseThreadLine(String s) {
        Matcher matcher = methodPattern.matcher(s);
        ThreaddumpParser.ThreadLineInfo threadLineInfo;
        if (matcher.lookingAt()) {
            MethodThreadLineInfo method = new MethodThreadLineInfo();
            String s1 = matcher.group(1);
            int i = s1.lastIndexOf('.');
            method.setClassName(s1.substring(0, i));
            method.methodName = s1.substring(i + 1);
            String s2 = matcher.group(2);
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
            threadLineInfo = method;
        } else if ((matcher = lockPattern.matcher(s)).lookingAt()) {
            LockThreadLineInfo lock = new LockThreadLineInfo();
            lock.type = lock.lookupType(matcher.group(1));
            lock.id = matcher.group(2);
            lock.className = matcher.group(3);
            threadLineInfo = lock;
        } else if(lockWithoutRef.equals(s)) {
            threadLineInfo = new MethodThreadLineInfo();
        } else {
            log.error("Unknown line: '" + s + "'");
            threadLineInfo = new MethodThreadLineInfo();
        }
        return threadLineInfo;
    }

    private final Pattern threadPattern = Pattern.compile("\"(.*?)\" (?:#\\d+ )?(daemon )?(?:prio=(\\S+) )?(?:os_prio=\\d+ )?(?:cpu=[\\w/.]+ )?(?:elapsed=[\\w/.]+ )?tid=(\\S+) nid=\\S+ ([^\\[]+)");
    private final Pattern methodPattern = Pattern.compile("\tat ([\\p{Alnum}$_.<>/]+)\\(([^\\)]*)\\)");
    private final Pattern lockPattern = Pattern.compile("\t- (waiting to lock|waiting on|locked|parking to wait for|eliminated|waiting to re-lock in wait\\(\\))\\s+<(\\p{Alnum}+)> \\(a ([^\\)]+)\\)");
    private final String lockWithoutRef = "\t- waiting on <no object reference available>";
}
