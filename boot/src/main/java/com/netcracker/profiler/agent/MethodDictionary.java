package com.netcracker.profiler.agent;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MethodDictionary {
    Map<String, Integer> map;
    ArrayList<String> methods;

    public MethodDictionary(int size) {
        map = new ConcurrentHashMap<String, Integer>((int) (size/0.75f));
        methods = new ArrayList<String>(size);
    }

    public int resolve(String methodName) {
        Integer methodId = map.get(methodName);
        if (methodId != null)
            return methodId;
        return createEntry(methodName);
    }

    private synchronized int createEntry(String methodName) {
        Integer methodId;
        methodId = methods.size();
        methods.add(methodName);
        map.put(methodName, methodId);
        return methodId;
    }

    public String resolve(int methodId) {
        return methods.get(methodId);
    }

    public ArrayList<String> getTags() {
        return methods;
    }
}
