package com.netcracker.profiler.io;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DurationParser {

    final static Pattern TIME_EXPR = Pattern.compile("^-?([0-9\\.]+)(d|day|days|h|ho|hou|hour|hours|m|mi|min|minu|minut|minute|minutes|s|se|sec|seco|secon|second|seconds)?(?:ago)?$");

    static long parseTime(String str, long def) {
        if ("now".equals(str)) return System.currentTimeMillis();

        final Matcher m = TIME_EXPR.matcher(str);
        if (!m.matches())
            return def;

        double value = Double.parseDouble(m.group(1));
        String unit = m.group(2);
        if (unit == null || unit.length() == 0)
            unit = "h";
        if (unit.startsWith("d"))
            value *= 24 * 3600 * 1000;
        else if (unit.startsWith("m"))
            value *= 60 * 1000;
        else if (unit.startsWith("s"))
            value *= 1000;
        else value *= 3600 * 1000;

        return (long) (System.currentTimeMillis() - value);
    }

    static long[] parseTimerange(String str) {
        str = str.replaceAll("\\s+", "");
        str = str.toLowerCase();
        String[] rangeBounds = str.split("\\.{2,}");
        long rangeLow = parseTime(rangeBounds[0], System.currentTimeMillis() - 3600 * 1000);
        long rangeHigh = rangeBounds.length > 1 ? parseTime(rangeBounds[1], System.currentTimeMillis()) : System.currentTimeMillis();
        if (rangeLow > rangeHigh) {
            long t = rangeLow;
            rangeLow = rangeHigh;
            rangeHigh = t;
        }
        return new long[]{rangeLow, rangeHigh};
    }

    final static Pattern DURATION_EXPR = Pattern.compile("-?(\\d+)\\s*(ms|mil|mo|m|s|h|d|w|y)?");

    public static long parseDuration(String str, long def) {
        str = str.toLowerCase();
        final Matcher m = DURATION_EXPR.matcher(str);
        long value = 0;
        boolean matched = false;
        while (m.find()) {
            matched = true;
            String unit = m.group(2);
            long qty = Long.parseLong(m.group(1));
            if (unit == null || unit.length() == 0)
                unit = "d";
            if (unit.startsWith("y"))
                qty *= 364L * 24 * 3600 * 1000;
            else if (unit.startsWith("mo"))
                qty *= 30L * 24 * 3600 * 1000;
            else if (unit.startsWith("w"))
                qty *= 7 * 24 * 3600 * 1000;
            else if (unit.startsWith("d"))
                qty *= 24 * 3600 * 1000;
            else if (unit.startsWith("h"))
                qty *= 3600 * 1000;
            else if (unit.equals("ms") || unit.startsWith("mil"))
                /* no need to convert ms to ms */ ;
            else if (unit.startsWith("m"))
                qty *= 60 * 1000;
            else if (unit.startsWith("s"))
                qty *= 1000;
            if (m.group(0).charAt(0) == '-')
                value -= qty;
            else
                value += qty;
        }
        if (!matched)
            return def;

        return value;
    }

    static long[] parseDurationRange(String str) {
        str = str.replaceAll("\\s+", "");
        str = str.toLowerCase();
        str = str.replaceAll("[<>=]", "");
        String[] rangeBounds = str.split("\\.{2,}");
        long rangeLow = parseDuration(rangeBounds[0], 100);
        long rangeHigh = rangeBounds.length > 1 ? parseDuration(rangeBounds[1], Long.MAX_VALUE) : Long.MAX_VALUE;
        if (rangeLow > rangeHigh) {
            long t = rangeLow;
            rangeLow = rangeHigh;
            rangeHigh = t;
        }
        return new long[]{rangeLow, rangeHigh};
    }

    public static long parseTimeInstant(String str, long def, long max, TimeZone tz) {
        try {
            long time = Long.parseLong(str);
            long now = System.currentTimeMillis();
            long maxDate = now + 1000L * 3600 * 24 * 365 * 2, minDate = now - 1000L * 3600 * 24 * 365 * 7;
            // Accept "up to 3 digits missing" or "up to 3 extra digits" in unix timestamp format
            for (int i = 0; i < 3; i++)
                if (Math.abs(now - time) > Math.abs(now - time * 10))
                    time *= 10;
                else break;
            for (int i = 0; i < 3; i++)
                if (Math.abs(now - time) > Math.abs(now - time / 10))
                    time /= 10;
                else break;
            if (time >= minDate && time <= maxDate)
                return time;
        } catch (NumberFormatException e) {
            /* ignore */
        }

        List<String> formats = Arrays.asList("yyyy-MM-dd HH:mm", "MM-dd HH:mm", "dd HH:mm", "HH:mm");
        for (int i = 0; i < formats.size(); i++) {
            String fmt = formats.get(i);
            long time;
            SimpleDateFormat sdf = new SimpleDateFormat(fmt);
            sdf.setTimeZone(tz);
            try {
                time = sdf.parse(str).getTime();
            } catch (ParseException e) {
                continue;
            }
            Calendar now = Calendar.getInstance();
            now.setTimeZone(tz);
            Calendar cal = Calendar.getInstance();
            cal.setTimeZone(tz);
            cal.setTimeInMillis(time);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            switch (i) {
                case 3:
                    cal.set(Calendar.DATE, now.get(Calendar.DATE));
                case 2:
                    cal.set(Calendar.MONTH, now.get(Calendar.MONTH));
                case 1:
                    cal.set(Calendar.YEAR, now.get(Calendar.YEAR));
                default:
            }
            time = cal.getTimeInMillis();
            if (time > max) {
                int field;
                if (fmt.startsWith("HH")) field = Calendar.DATE;
                else if (fmt.startsWith("dd")) field = Calendar.MONTH;
                else field = Calendar.YEAR;
                while (time > max) {
                    cal.add(field, -1);
                    time = cal.getTimeInMillis();
                }
            }
            return time;
        }

        if ("now".equalsIgnoreCase(str)) {
            return System.currentTimeMillis();
        }

        long offs = parseDuration(str, Long.MAX_VALUE);
        if (offs != Long.MAX_VALUE) {
            return System.currentTimeMillis() - offs;
        }

        return def;
    }

}
