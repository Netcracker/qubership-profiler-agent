package com.netcracker.profiler.sax.raw;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.sax.values.ClobValue;

import java.io.IOException;

public abstract class AbstractClobReaderFlyweight implements ClobReaderFlyweight {
    private String currentFolder;
    private DataInputStreamEx is;
    private IOException ioException;
    private int fileIndex;
    private int length;
    private int startPosition;

    protected abstract DataInputStreamEx reopenDataInputStream(DataInputStreamEx oldOne, String folder, int fileIndex) throws IOException;

    public void adaptTo(ClobValue clob) {
        if (!clob.folder.equals(currentFolder) || clob.fileIndex != fileIndex
                || (is != null && is.position() > clob.offset)) {
            try {
                ioException = null;
                currentFolder = clob.folder;
                fileIndex = clob.fileIndex;
                is = reopenDataInputStream(is, clob.folder, clob.fileIndex);
            } catch (IOException e) {
                ioException = e;
                return;
            }
        }
        if (is == null) {
            ioException = new IOException("IllegalState: no input stream while adapting to clob " + String.valueOf(clob) + " in folder " + String.valueOf(currentFolder));
            return;
        }
        if (is.position() < clob.offset) {
            try {
                is.skipBytes(clob.offset - is.position());
            } catch (IOException e) {
                ioException = e;
            }
        }
        try {
            length = is.readVarInt();
        } catch (IOException e) {
            ioException = e;
        }
        startPosition = is.position();
    }

    public int length() {
        return length;
    }

    public CharSequence subSequence(int start, int end) throws IOException {
        if (ioException != null) throw ioException;
        if (end > length) throw new IOException("End index " + end + " exceeds charSequence length " + length);
        if (start < 0) throw new IOException("Start index " + start + " is negative");
        if (end - start < 1) return ""; // empty result on zero character read
        int offset = startPosition - is.position() + start;
        if (offset < 0) {
            throw new IOException("Negative seeks not implemented. "
                    + "String start position: " + startPosition + ", stream position: " + is.position() + ", start: " + start + ", end: " + end);
        }
        is.skipBytes(offset * 2);

        char[] ary = new char[end - start];
        for (int i = 0; i < ary.length; i++) {
            ary[i] = is.readChar();
        }

        return new String(ary);
    }

    @Override
    public String toString() {
        return "ClobReaderFlyweightFile{" +
                "currentFolder='" + currentFolder + '\'' +
                ", fileIndex=" + fileIndex +
                ", length=" + length +
                ", startPosition=" + startPosition +
                '}';
    }
}
