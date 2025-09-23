package com.netcracker.profiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class ServerNameResolver {
    private static final Logger logger = LoggerFactory.getLogger(ServerNameResolver.class);

    public static final String PARAM_POD_NAME = "POD_NAME";
    public static final String DEFAULT_SERVER_NAME = "default";

    public static final String POD_NAME = parsePODName();
    public static final String SERVER_NAME = System.getProperty("com.netcracker.esc.serverName", System.getProperty("weblogic.Name", System.getProperty("jboss.server.name", POD_NAME)));
    static {
        if (System.getProperty("com.netcracker.esc.serverName") == null) {
            System.setProperty("com.netcracker.esc.serverName", SERVER_NAME);
        }
    }

    public static String parsePODName() {
        String result = getPropertyOrEnvVariable(PARAM_POD_NAME);
        if (!isBlank(result)) {
            return result;
        }

        try (Scanner s = new Scanner(Runtime.getRuntime().exec("hostname").getInputStream()).useDelimiter("\\A");) {
            if(s.hasNext()) {
                return s.next().trim();
            }
        } catch (Throwable t) {
            logger.warn("Exception in getting hostname", t);
        }
        return DEFAULT_SERVER_NAME;
    }

    public static String getPropertyOrEnvVariable(String paramName) {
        String result = System.getProperty(paramName);
        if (!isBlank(result)) {
            return result.trim();
        }
        result = System.getenv(paramName);
        if (!isBlank(result)) {
            return result.trim();
        }
        return null;
    }

    public static int getPropertyOrEnvVariable(String paramName, int defaultValue){
        String strVal = getPropertyOrEnvVariable(paramName);
        if(strVal == null || strVal.length() == 0) {
            return defaultValue;
        }
        return Integer.parseInt(strVal);
    }

    public static String getPropertyOrEnvVariable(String paramName, String defaultValue) {
        String strVal = getPropertyOrEnvVariable(paramName);
        if (strVal == null || strVal.length() == 0) {
            return defaultValue;
        }
        return strVal;
    }

    private static boolean isBlank(CharSequence cs) {
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

}
