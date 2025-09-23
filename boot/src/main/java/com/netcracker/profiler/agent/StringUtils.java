package com.netcracker.profiler.agent;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class StringUtils {
    public static String left(String s, int len) {
        return s == null || s.length() < len ? s : s.substring(0, len);
    }

    public static String right(String s, int len) {
        return s == null || s.length() < len ? s : s.substring(s.length() - len);
    }

    public static StringBuffer throwableToString(Throwable t) {
        if (t == null) return new StringBuffer();
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.getBuffer();
    }

    public static StringBuilder arrayToString(StringBuilder sb, Object[] a) {
        if (a == null)
            return sb.append("null");
        if (a.length == 0)
            return sb.append("[]");
        int max = a.length;
        sb.append('[').append(a[0]);
        int prev = 0; Object prevObject = a[0];
        for(int i=1; i<max; i++) {
            Object ai = a[i];
            if (ai == prevObject || ai != null && ai.equals(prevObject))
                continue;

            if (i == prev + 1)
                sb.append(", ");
            else {
                sb.append(" (").append(i - prev).append(" times), (").append(i).append("):");
            }
            prev = i;
            prevObject = ai;
            sb.append(ai);
        }
        if (prev < max - 1) {
            sb.append(" (").append(max - prev).append(" times)");
        }
        return sb.append(']');
    }

    public static StringBuilder arrayToString(StringBuilder sb, long[] a) {
        if (a == null)
            return sb.append("null");
        if (a.length == 0)
            return sb.append("[]");
        int max = a.length;
        sb.append('[').append(a[0]);
        int prev = 0; long prevObject = a[0];
        for(int i=1; i<max; i++) {
            long ai = a[i];
            if (ai == prevObject)
                continue;

            if (i == prev + 1)
                sb.append(", ");
            else {
                sb.append(" (").append(i - prev).append(" times), (").append(i).append("):");
            }
            prev = i;
            prevObject = ai;
            sb.append(ai);
        }
        if (prev < max - 1) {
            sb.append(" (").append(max - prev).append(" times)");
        }
        return sb.append(']');
    }

    public static Object convert(Object o) {
        if (o == null)
            return "null";
        if (o instanceof String)
            return o;
        if (o instanceof StringBuffer || o instanceof StringBuilder || o instanceof Number || o instanceof Boolean)
            return o.toString();
        if (o instanceof Throwable)
            return throwableToString((Throwable) o);
        try {
            if (o instanceof Collection || o instanceof Map)
                return o.toString();

            return convertRareType(o);
        } catch (Throwable t) {
            return throwableToString(t);
        }
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    private static Object convertRareType(Object o) {
        if (o instanceof Object[])
            return Arrays.deepToString((Object[]) o);
        Class k = o.getClass();
        if (k.isArray())
            return Arrays.deepToString(new Object[]{o});
        return o.toString();
    }

    static boolean stringDiffers(String a, String b) {
        return (a == null && b != null) || (a != null && !a.equals(b));
    }

    static String cap(String a, int maxLength) {
        return a == null || a.length() <= maxLength ? a : a.substring(0, maxLength);
    }
}
