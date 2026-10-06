package com.netcracker.profiler.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionUtils {
    public static final Pattern NUMBER = Pattern.compile("(\\d+)");

    /**
     * Prepares string for natural sorting via padding of all numbers to numWidth characters
     * For instance, for source string "1.23b2.1" will return "001.023b002.001" when padding is equal to 3
     * @param src source string to be prepared for natural sorting
     * @param numWidth maximal width of numbers in
     * @return string with all numbers replaced with padded versions
     */
    public static String naturalOrder(String src, int numWidth) {
        if (src != null && src.length() > 0 && src.charAt(0) == 'v')
            src = src.substring(1);
        Matcher m = NUMBER.matcher(src);
        StringBuffer sb = new StringBuffer();
        StringBuffer rep = new StringBuffer();
        while (m.find()) {
            int len = m.end() - m.start();
            rep.setLength(0);
            for (int i = len; i < numWidth; i++)
                rep.append('0');
            rep.append("$1");
            m.appendReplacement(sb, rep.toString());
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
