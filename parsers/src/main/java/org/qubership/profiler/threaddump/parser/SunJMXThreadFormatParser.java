package org.qubership.profiler.threaddump.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for jstat (sun) generated thread dumps
 */
public class SunJMXThreadFormatParser implements ThreadFormatParser {
    private static final Logger log = LoggerFactory.getLogger(SunJMXThreadFormatParser.class);
    public ThreadInfo parseThread(String s) {
        ThreadInfo threadinfo = new ThreadInfo();
        Matcher matcher = threadPattern.matcher(s);
        if (matcher.lookingAt()) {
            threadinfo.name = matcher.group(1);
            threadinfo.state = matcher.group(2).trim();
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
        } else {
            log.error("Unknown line: '" + s + "'");
            threadLineInfo = new MethodThreadLineInfo();
        }
        return threadLineInfo;
    }

    private final Pattern threadPattern = Pattern.compile("\"(.*?)\" (waiting to lock|waiting on|locked|parking to wait for|eliminated)?");
    private final Pattern methodPattern = Pattern.compile("\t([\\p{Alnum}$_.<>/]+)\\(([^\\)]*)\\)");
}
