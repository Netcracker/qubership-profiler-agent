package com.netcracker.profiler.agent;

import java.lang.reflect.Field;

public class JarIndexExtraInfo {
    public static void addFileName(Object loader, String resource, boolean valid) {
        if (valid) {
            return;
        }
        Class<?> aClass = loader.getClass();
        Field csuField = null;
        Object fileUrl = "unknown";
        try {
            csuField = aClass.getDeclaredField("csu");
            csuField.setAccessible(true);
        } catch (NoSuchFieldException e) {
        }
        try {
            if (csuField != null) {
                fileUrl = csuField.get(loader);
            }
        } catch (IllegalAccessException e) {
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Invalid JAR index detected: ").append(fileUrl).append(", requested resource: ").append(resource)
                .append(". At least one resource with the package ");
        int pos = resource.lastIndexOf('/');
        if (pos == -1) {
            sb.append(resource);
        } else {
            sb.append(resource, 0, pos);
        }
        sb.append(" should present in the jar");
        throw new RuntimeException(sb.toString());
    }
}
