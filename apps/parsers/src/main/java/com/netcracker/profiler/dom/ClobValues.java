package com.netcracker.profiler.dom;

import com.netcracker.profiler.sax.values.ClobValue;

import java.util.*;

public class ClobValues {
    private List<ClobValue> list = new ArrayList<ClobValue>();
    private Set<ClobValue> observedClobs;

    public void add(ClobValue clob) {
        list.add(clob);
    }

    public Collection<ClobValue> getClobs() {
        return list;
    }

    public void merge(ClobValues clobValues) {
        Collection<ClobValue> other = clobValues.getClobs();
        if (observedClobs == null) {
            observedClobs = new HashSet<ClobValue>((int) ((list.size() + other.size())/0.70f));
            observedClobs.addAll(list);
        }
        for (ClobValue clob : other) {
            if (observedClobs.add(clob)) {
                list.add(clob);
            }
        }
    }
}
