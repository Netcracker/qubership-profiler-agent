package com.netcracker.profiler.io;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SizeParser {
    final static Pattern SIZE_EXPR = Pattern.compile("-?(\\d+)\\s*(b|k|m|g|t)?");

    public static long parseSize(String str, long def) {
        str = str.toLowerCase();
        final Matcher m = SIZE_EXPR.matcher(str);
        long value = 0;
        boolean matched = false;
        while (m.find()) {
            matched = true;
            String unit = m.group(2);
            long qty = Long.parseLong(m.group(1));
            if (unit == null || unit.length() == 0)
                unit = "m";
            switch (unit.charAt(0)) {
                case 't':
                    qty *= 1024;
                case 'g':
                    qty *= 1024;
                case 'm':
                    qty *= 1024;
                case 'k':
                    qty *= 1024;
            }
            if (m.group(0).charAt(0) == '-')
                value -= qty;
            else
                value += qty;
        }
        if (!matched)
            return def;

        return value;
    }
}
