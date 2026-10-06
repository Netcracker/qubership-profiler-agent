package com.netcracker.profiler.agent;

public class PropertyFacadeBoot {
    public static String getProperty(String name, String defaultValue) {
        String result = getStringPropertyInternal(name, null);
        if (result != null)
            return result;

        name = name.replace("com.netcracker.profiler.agent", "com.netcracker.profiler");
        return getStringPropertyInternal(name, defaultValue);
    }

    private static String getStringPropertyInternal(String name, String defaultValue) {
        String result = System.getProperty(name);
        if (result != null)
            return result;

        name = name.replace("profiler", "execution-statistics-collector");
        return System.getProperty(name, defaultValue);
    }

    public static int getProperty(String name, int defaultValue) {
        Integer result = getIntPropertyInternal(name, null);
        if (result != null)
            return result;
        name = name.replace("com.netcracker.profiler.agent", "com.netcracker.profiler");
        return getIntPropertyInternal(name, defaultValue);
    }

    private static Integer getIntPropertyInternal(String name, Integer defaultValue) {
        Integer result = Integer.getInteger(name);
        if (result != null)
            return result;

        name = name.replace("profiler", "execution-statistics-collector");
        return Integer.getInteger(name, defaultValue);
    }

    public static boolean getProperty(String name, boolean defaultValue) {
        return Boolean.valueOf(getProperty(name, Boolean.toString(defaultValue)));
    }

    public static String getPropertyOrEnvVariable(String paramName) {
        String result = System.getProperty(paramName);
        if (!StringUtils.isBlank(result)) {
            return result.trim();
        }
        result = System.getenv(paramName);
        if (!StringUtils.isBlank(result)) {
            return result.trim();
        }
        return null;
    }
}
