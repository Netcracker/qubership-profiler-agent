package com.netcracker.profiler.configuration;

public class PropertyFacade {
    public static String getProperty(String name, String defaultValue) {
        String result = System.getProperty(name);
        if (result != null)
            return result;

        name = name.replace("profiler", "execution-statistics-collector");
        return System.getProperty(name, defaultValue);
    }

    public static int getProperty(String name, int defaultValue) {
        Integer result = Integer.getInteger(name);
        if (result != null)
            return result;

        name = name.replace("profiler", "execution-statistics-collector");
        return Integer.getInteger(name, defaultValue);
    }

    public static boolean getProperty(String name, boolean defaultValue) {
        return Boolean.valueOf(getProperty(name, Boolean.toString(defaultValue)));
    }
}
