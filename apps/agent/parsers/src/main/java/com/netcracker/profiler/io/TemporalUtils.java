package com.netcracker.profiler.io;


import jakarta.servlet.http.HttpServletRequest;

public class TemporalUtils {
    public static TemporalRequestParams parseTemporal(HttpServletRequest request){
        TemporalRequestParams result = new TemporalRequestParams();
        long now = System.currentTimeMillis();
        result.now = now;
        if("last2min".equals(request.getParameter("last2MinOrRange"))){
            result.timerangeFrom = now - 3*60*1000;
            result.timerangeTo = now;
        } else {
            result.timerangeFrom = parseLong(request, "timerange[min]", result.now - 15 * 60 * 1000);
            result.timerangeTo = parseLong(request, "timerange[max]", result.now);
        }

        result.serverUTC = now;
        result.clientUTC = parseLong(request, "clientUTC", result.serverUTC);
        result.autoUpdate = parseLong(request, "timerange[autoUpdate]", 1);

        result.durationFrom = parseLong(request, "duration[min]", 500);
        result.durationTo = parseLong(request, "duration[max]", Long.MAX_VALUE);
        return result;
    }

    public static long parseLong(HttpServletRequest request, String paramName, long defaultValue) throws IllegalArgumentException {
        final String s = request.getParameter(paramName);
        if (s == null || s.length() == 0)
            return defaultValue;
        try {
            return Long.parseLong(s);
        } catch (Throwable t) {
            return defaultValue;
        }
    }

}
