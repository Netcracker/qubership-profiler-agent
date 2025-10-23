package com.netcracker.common.models.meta;

import com.netcracker.profiler.sax.io.DataInputStreamEx;

import java.io.IOException;
import java.util.*;

public class ClobIndex {
    private final int maxLength;
    private Map<Value.ClobId, Value.Clob> unique = new HashMap<>();
    private List<Value.Clob> loaded = new ArrayList<>(); // (not sorted) in order of loading
    private Set<Value.Clob> observedClobs;

    public ClobIndex(int maxLength) {
        this.maxLength = maxLength;
    }

    public List<Value.Clob> uniqToLoad() { // only not loaded yet (with empty value)
        return unique.values().stream()
                .filter(Value.Clob::isEmpty)
                .sorted().toList();
    }

    public Value getOrDefault(Value.ClobId id, Value.Clob newClob) {
        var existingClob = unique.get(id);
        if (existingClob != null) {
            return existingClob;
        } else {
            unique.put(id, newClob);
            return newClob;
        }
    }

    public boolean has(Value.ClobId id) {
        return unique.get(id) != null;
    }

    public CharSequence text(Value.ClobId id) {
        var existingClob = unique.get(id);
        if (existingClob != null) {
            return existingClob.get();
        }
        return null;
    }

    public void load(Value.Clob clob, DataInputStreamEx is) throws IOException {
        clob.readFrom(is, maxLength); // override value (atomic)
        loaded.add(clob);
    }

    public void merge(ClobIndex clobValues) {
        Collection<Value.Clob> other = clobValues.getClobs();
        if (observedClobs == null) {
            observedClobs = new HashSet<>((int) ((loaded.size() + other.size()) / 0.70f));
            observedClobs.addAll(loaded);
        }
        for (var clob : other) {
            if (observedClobs.add(clob)) {
                loaded.add(clob);
                unique.put(clob.id(), clob);
            }
        }
    }

    public Collection<Value.Clob> getClobs() {
        return loaded;
    }

}
