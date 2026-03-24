package com.netcracker.common.models.pod.stat;


public class BlobSize {
    long original = 0;
    long compressed = 0;

    public BlobSize(long orig, long compress) {
        this.original = orig;
        this.compressed = compress;
    }

    public long val(boolean forOriginal) {
        return forOriginal ? original : compressed;
    }

    public BlobSize append(boolean forOriginal, long val) {
        if (forOriginal) {
            original += val;
        } else {
            compressed += val;
        }
        return this;
    }

    public static BlobSize empty() {
        return new BlobSize(0, 0);
    }

    public static BlobSize of(long original, long compressed) {
        return new BlobSize(original, compressed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlobSize blobSize = (BlobSize) o;
        if (original != blobSize.original) return false;
        return compressed == blobSize.compressed;
    }

    @Override
    public int hashCode() {
        int result = (int) (original ^ (original >>> 32));
        result = 31 * result + (int) (compressed ^ (compressed >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "BlobSize{orig=" + original + ", zip=" + compressed + '}';
    }

    static BlobSize max(BlobSize a1, BlobSize a2) {
        return (a1.original > a2.original) ? a1 : a2;
    }

    static BlobSize min(BlobSize a1, BlobSize a2) {
        return (a1.original < a2.original) ? a1 : a2;
    }

    static BlobSize diff(BlobSize a1, BlobSize a2) {
        return BlobSize.of( Math.abs(a2.original - a1.original) , Math.abs(a2.compressed - a1.compressed) );
    }

    static BlobSize MAX = new BlobSize(Long.MAX_VALUE, Long.MAX_VALUE);
    static BlobSize MIN = new BlobSize(0, 0);

    public void override(long l) {  // TODO hack for go collector
        original = l;
        compressed = l;
    }
}
