package com.netcracker.profiler.model;

import java.util.List;
import java.util.Map;

public record CallRowId(String file, TreeRowId treeRow) implements Comparable<CallRowId> {

    public static CallRowId parse(String q, Map<String, Object> params) {

        // q ::= fullAddress _ traceFileIndex _ bufferOffset _ recordIndex // OLD: _ reactorFileIndex _ reactorBufferOffset
        final String[] str = q.split("_");

        var key = "f[_" + str[0] + "]";
        if (params.get(key) == null) {
            return null;
        }
        var file = retrieveFile(params.get(key));
        if (file == null) {
            return null;
        }
        var rowid = new TreeRowId(
                Integer.parseInt(str[0]),
                q,
                Integer.parseInt(str[1]),
                Integer.parseInt(str[2]),
                Integer.parseInt(str[3])
        );
        return new CallRowId(file, rowid);
    }

    private static String retrieveFile(Object o) {
        String file = null;
        if (o instanceof List<?> l) {
            var val = l.get(0);
            if (val instanceof String s) {
                file = s;
            }
        }
        return file;
    }

    public int compareTo(CallRowId o) {
        final int i = file.compareTo(o.file);
        if (i != 0) return i;
        return treeRow.compareTo(o.treeRow);
    }
}
