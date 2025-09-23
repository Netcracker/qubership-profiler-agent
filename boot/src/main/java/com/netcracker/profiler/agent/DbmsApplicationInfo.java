package com.netcracker.profiler.agent;

import java.util.regex.Pattern;

public class DbmsApplicationInfo {
    private final static String SERVER_NAME = System.getProperty("weblogic.Name", System.getProperty("jboss.server.name", ""));

    public static String generateActionString(String object, String tab) {
        return object + ':' + tab;
    }


    public final static Pattern EXECUTE_THREAD = Pattern.compile("ExecuteThread: ");

    public static String generateClientInfo(String xid) {
        if (xid != null && xid.startsWith("BEA1-"))
            xid = xid.substring(5); // remove BEA1-

        String thread = Thread.currentThread().getName();
        thread = EXECUTE_THREAD.matcher(thread).replaceAll("");

        return xid + ':' + SERVER_NAME + ':' + thread;
    }
}
