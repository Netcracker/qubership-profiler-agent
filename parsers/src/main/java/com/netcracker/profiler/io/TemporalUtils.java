package com.netcracker.profiler.io;


import javax.servlet.http.HttpServletRequest;

public class TemporalUtils {
    public static TemporalRequestParams parseTemporal(HttpServletRequest request){
        TemporalRequestParams result = new TemporalRequestParams();
        if("last2min".equals(request.getParameter("last2MinOrRange"))){
            result.timerangeFrom = System.currentTimeMillis() - 3*60*1000;
            result.timerangeTo = System.currentTimeMillis();
        } else {
            result.timerangeFrom = parseLong(request, "timerange[min]", result.now - 15 * 60 * 1000);
            result.timerangeTo = parseLong(request, "timerange[max]", result.now);
        }

        result.now = System.currentTimeMillis();
        result.serverUTC = result.now;
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
