package com.netcracker.profiler.sax.values;

public class ClobValue extends ValueHolder implements Comparable<ClobValue> {
    public final String dataFolderPath;
    public final String folder;
    public final int fileIndex;
    public final int offset;
    public CharSequence value;

    public ClobValue(String dataFolderPath, String folder, int fileIndex, int offset) {
        this.dataFolderPath = dataFolderPath;
        this.folder = folder;
        this.fileIndex = fileIndex;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClobValue clobValue = (ClobValue) o;

        if (fileIndex != clobValue.fileIndex) return false;
        if (offset != clobValue.offset) return false;
        if (!folder.equals(clobValue.folder)) return false;
        if (!dataFolderPath.equals(clobValue.dataFolderPath)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = folder.hashCode();
        result = 31 * result + fileIndex;
        result = 31 * result + offset;
        result = 31 * result + dataFolderPath.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClobValue{" +
                "folder='" + folder + '\'' +
                ", fileIndex=" + fileIndex +
                ", offset=" + offset +
                ", dataFolder=" + dataFolderPath +
                '}';
    }

    public int compareTo(ClobValue o) {
        if (!folder.equals(o.folder))
            return folder.compareTo(o.folder);
        if (fileIndex != o.fileIndex) return fileIndex - o.fileIndex;
        if (offset != o.offset) return offset - o.offset;
        return dataFolderPath.compareTo(o.dataFolderPath);
    }
}
