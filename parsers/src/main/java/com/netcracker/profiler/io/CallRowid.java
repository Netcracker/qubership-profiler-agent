package com.netcracker.profiler.io;

import com.netcracker.profiler.sax.raw.TreeRowid;

import java.util.Map;

public class CallRowid implements Comparable<CallRowid> {
    public final String file;
    public final TreeRowid rowid;

    public CallRowid(String q, Map params) {
        final String[] str = q.split("_");
        file = ((String[]) params.get("f[_" + str[0] + "]"))[0];
        rowid = new TreeRowid(
                Integer.parseInt(str[0]),
                q,
                Integer.parseInt(str[1]),
                Integer.parseInt(str[2]),
                Integer.parseInt(str[3]),
                str.length>=5 ? Integer.parseInt(str[4]) : 0,
                str.length>=6 ? Integer.parseInt(str[5]) : 0
        );
    }
    public CallRowid(String file, int folderId, int traceFileIndex, int bufferOffset, int recordIndex, int reactorFileIndex, int reactorBufferOffset) {
        this.file = file;
        String fullAddress = "_" +
                traceFileIndex + "_" +
                bufferOffset + "_" +
                recordIndex + "_" +
                reactorFileIndex + "_" +
                reactorBufferOffset + "_";
        rowid = new TreeRowid(
                folderId,
                fullAddress,
                traceFileIndex,
                bufferOffset,
                recordIndex,
                reactorFileIndex,
                reactorBufferOffset
        );
    }

    public CallRowid(String file, TreeRowid treeRowid) {
        this.file = file;
        this.rowid = treeRowid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CallRowid callRowid = (CallRowid) o;
        return rowid.equals(callRowid.rowid) && file.equals(callRowid.file);
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + rowid.hashCode();
        return result;
    }

    public int compareTo(CallRowid o) {
        final int i = file.compareTo(o.file);
        if (i != 0) return i;

        return rowid.compareTo(o.rowid);
    }
}
