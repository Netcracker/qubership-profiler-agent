package com.netcracker.profiler.model;

// sorted by [file, bufferOffset, recordIndex]
public class TreeRowId implements Comparable<TreeRowId> {
    public final int folderId;
    public final String fullRowId;
    public final int traceFileIndex;
    public final int bufferOffset;
    public final int recordIndex;

    public final static TreeRowId UNDEFINED = new TreeRowId(0,null,0, 0, 0);

    public TreeRowId(int folderId, String fullRowId, int traceFileIndex, int bufferOffset, int recordIndex) {
        this.folderId = folderId;
        this.fullRowId = fullRowId;
        this.traceFileIndex = traceFileIndex;
        this.bufferOffset = bufferOffset;
        this.recordIndex = recordIndex;
    }

    public int compareTo(TreeRowId o) {
        if (traceFileIndex != o.traceFileIndex)
            return traceFileIndex < o.traceFileIndex ? -1 : 1;

        if (bufferOffset != o.bufferOffset)
            return bufferOffset < o.bufferOffset ? -1 : 1;

        if (recordIndex != o.recordIndex)
            return recordIndex < o.recordIndex ? -1 : 1;

        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TreeRowId treeRowid = (TreeRowId) o;

        if (bufferOffset != treeRowid.bufferOffset) return false;
        if (recordIndex != treeRowid.recordIndex) return false;
        if (traceFileIndex != treeRowid.traceFileIndex) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = traceFileIndex;
        result = 31 * result + bufferOffset;
        result = 31 * result + recordIndex;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TreeRowid{");
        sb.append("traceFileIndex=").append(traceFileIndex);
        sb.append(", bufferOffset=").append(bufferOffset);
        sb.append(", recordIndex=").append(recordIndex);
        sb.append(", fullRowId='").append(fullRowId).append('\'');
        sb.append(", folderId=").append(folderId);
        sb.append('}');
        return sb.toString();
    }
}
