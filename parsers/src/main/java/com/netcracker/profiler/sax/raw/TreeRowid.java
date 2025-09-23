package com.netcracker.profiler.sax.raw;

public class TreeRowid implements Comparable<TreeRowid> {
    public final int traceFileIndex;
    public final int bufferOffset;
    public final int recordIndex;
    public final int reactorFileIndex;
    public final int reactorBufferOffset;
    public final String fullRowId;
    public final int folderId;

    public final static TreeRowid UNDEFINED = new TreeRowid(0,null,0, 0, 0, 0, 0);


    public TreeRowid(int folderId,
                     String fullRowId,
                     int traceFileIndex,
                     int bufferOffset,
                     int recordIndex,
                     int reactorFileIndex,
                     int reactorBufferOffset) {
        this.folderId = folderId;
        this.fullRowId = fullRowId;
        this.traceFileIndex = traceFileIndex;
        this.bufferOffset = bufferOffset;
        this.recordIndex = recordIndex;
        this.reactorFileIndex = reactorFileIndex;
        this.reactorBufferOffset = reactorBufferOffset;
    }

    public int compareTo(TreeRowid o) {
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

        TreeRowid treeRowid = (TreeRowid) o;

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
        sb.append(", reactorFileIndex=").append(reactorFileIndex);
        sb.append(", reactorBufferOffset=").append(reactorBufferOffset);
        sb.append(", fullRowId='").append(fullRowId).append('\'');
        sb.append(", folderId=").append(folderId);
        sb.append('}');
        return sb.toString();
    }
}
