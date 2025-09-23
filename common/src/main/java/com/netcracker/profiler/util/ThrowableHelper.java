package com.netcracker.profiler.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ThrowableHelper {
    public static String throwableToString(Throwable t) {
        if (t == null) {
            return "null exception";
        }
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
