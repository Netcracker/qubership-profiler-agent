package com.netcracker.profiler.util;

public class TimeHelper {
    public static StringBuilder humanizeDifference(StringBuilder sb, long diff) {
        if (diff == 0) return sb;
        if (sb == null) sb = new StringBuilder();
        long diffInSeconds = Math.abs(diff) / 1000;
        long sec = (diffInSeconds >= 60 ? diffInSeconds % 60 : diffInSeconds);
        long min = (diffInSeconds = (diffInSeconds / 60)) >= 60 ? diffInSeconds % 60 : diffInSeconds;
        long hour = (diffInSeconds = (diffInSeconds / 60)) >= 24 ? diffInSeconds % 24 : diffInSeconds;
        long day = (diffInSeconds = (diffInSeconds / 24));
        boolean started = false;
        started = printTimeComponent(sb, day, " day", started);
        started = printTimeComponent(sb, hour, " hour", started);
        started = printTimeComponent(sb, min, " minute", started);
        if (!started && sec == 0) sec = 1;
        printTimeComponent(sb, sec, " second", started);
        sb.append(diff > 0 ? " ahead" : " behind");
        return sb;
    }

    private static boolean printTimeComponent(StringBuilder sb, long component, String name, boolean started) {
        if (component == 0 && !started) return false;
        if (started) sb.append(" and");
        sb.append(' ');
        sb.append(component);
        sb.append(name);
        if (component > 1)
            sb.append('s');
        return true;
    }

}
