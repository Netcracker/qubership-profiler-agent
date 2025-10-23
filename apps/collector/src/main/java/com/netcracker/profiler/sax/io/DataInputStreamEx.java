package com.netcracker.profiler.sax.io;

import com.netcracker.profiler.timeout.ReadInterruptedException;

import java.io.*;
import java.text.NumberFormat;

public class DataInputStreamEx extends FilterInputStream implements IDataInputStreamEx {
    final static NumberFormat fileIndexFormat = NumberFormat.getIntegerInstance();

    static {
        fileIndexFormat.setGroupingUsed(false);
        fileIndexFormat.setMinimumIntegerDigits(6);
    }

    private int position = 0;

    /**
     * Creates a DataInputStream that uses the specified underlying InputStream.
     */
    public DataInputStreamEx(InputStream in) {
        super(in);
    }

    public boolean isEmpty() {
        return this.in == null;
    }

    public String readString() throws IOException {
        return readString(100 * 1024 * 1024);
    }

    public String readString(int maxLength) throws IOException {
        int length = readVarInt();
        if (length > maxLength) {
            throw new IOException("Expecting string of max length " + maxLength + ", got " + length
                    + " chars; position = " + position);
        }
        char[] x = new char[length];
        for (int i = 0; i < length; i++)
            x[i] = readChar();
        return new String(x);
    }

    public char readChar() throws IOException {
        int c1 = read();
        int c2 = read();
        if ((c1 | c2) < 0) throw new EOFException();
        return (char) ((c1 << 8) | c2);
    }

    public short readShort() throws IOException {
        int c1 = read();
        int c2 = read();
        if ((c1 | c2) < 0) throw new EOFException();
        return (short) ((c1 << 8) | c2);
    }

    public int readInt() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private char[] charBuffer = new char[8];
    private byte[] byteBuffer = new byte[8];

    public long readLong() throws IOException {
        final byte[] buffer = byteBuffer;
        readFully(buffer, 0, 8);
        return (((long) buffer[0] << 56) +
                ((long) (buffer[1] & 255) << 48) +
                ((long) (buffer[2] & 255) << 40) +
                ((long) (buffer[3] & 255) << 32) +
                ((long) (buffer[4] & 255) << 24) +
                ((buffer[5] & 255) << 16) +
                ((buffer[6] & 255) << 8) +
                ((buffer[7] & 255)));
    }

    public void readFully(byte[] buffer, int pos, int len) throws IOException {
        while (len > 0) {
            final int bytesRead = read(buffer, pos, len);
            if (bytesRead < 0)
                throw new EOFException();
            pos += bytesRead;
            len -= bytesRead;
        }
    }

    public int readVarInt() throws IOException {
        int res = read(), x;
        if (res == -1) throw new EOFException();
        if ((res & 0x80) == 0) return res;
        res &= ~0x80;
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 7;
        if ((res & (0x80 << 7)) == 0) return res;
        res &= ~(0x80 << 7);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 14;
        if ((res & (0x80 << 14)) == 0) return res;
        res &= ~(0x80 << 14);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 21;
        if ((res & (0x80 << 21)) == 0) return res;
        res &= ~(0x80 << 21);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 28;
        return res;
    }

    public long readVarLong() throws IOException {
        int res = read(), x;
        if (res == -1) throw new EOFException();
        if ((res & 0x80) == 0) return res;
        res &= ~0x80; // 0..6
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 7;
        if ((res & (0x80 << 7)) == 0) return res;
        res &= ~(0x80 << 7); // 7..13
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 14;
        if ((res & (0x80 << 14)) == 0) return res;
        res &= ~(0x80 << 14); // 14..20
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 21;
        if ((res & (0x80 << 21)) == 0) return res;
        res &= ~(0x80 << 21); // 21..28
        if ((x = read()) == -1) throw new EOFException();
        if ((x & 0x80) == 0) return (((long) x) << 28) | res;
        long resLong = (((long) (x & 0x7f)) << 28) | res;

        return (((long) readVarInt()) << 35) | resLong;
    }

    public final int readVarIntZigZag() throws IOException {
        int res = readVarInt();
        return (res >>> 1) ^ (-(res & 1));
    }

    public final long readVarLongZigZag() throws IOException {
        long res = readVarLong();
        return (res >>> 1) ^ (-(res & 1));
    }

    public int position() {
        return position;
    }

    private void checkInterrupted(){
        if(Thread.interrupted()) {
            throw new ReadInterruptedException();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        checkInterrupted();
        final long bytesRead = super.skip(n);
        position += bytesRead;
        return bytesRead;
    }

    @Override
    public int read() throws IOException { // read next byte
        checkInterrupted();
        final int i = super.read();
        position++;
        return i;
    }

    @Override
    public int read(byte[] b) throws IOException { // beware: conflicts with `read(byte[] b, int off, int len)` (incorrect position)
        checkInterrupted();
        final int bytesRead = super.read(b);
        position += bytesRead;
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkInterrupted();
        final int bytesRead = super.read(b, off, len);
        position += bytesRead;
        return bytesRead;
    }

    public void skipBytes(int bytes) throws IOException {
        while (bytes > 0) {
            long skipped = skip(bytes);
            if(skipped == 0){
                throw new EOFException();
            }
            bytes -= skipped;
        }
    }

    public void skipString() throws IOException {
        int length = readVarInt();
        skipBytes(length * 2);
    }

    public int available() throws IOException {
        return in.available();
    }

    public void reset() throws IOException {
        in.reset();
    }

    @Override
    public void close() throws IOException {
        //protect against NPE when closing a stream that failed to open
        if (in != null) {
            super.close();
        }
    }
}
