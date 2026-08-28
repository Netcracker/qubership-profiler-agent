package com.netcracker.profiler.dump;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStreamEx extends DataOutputStream implements IDataOutputStreamEx {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see java.io.FilterOutputStream#out
     */
    public DataOutputStreamEx(OutputStream out) {
        super(out);
    }

    int prevStringOffset;

    @Override
    public int getPrevStringOffset() {
        return prevStringOffset;
    }

    @Override
    public int write(String s) throws IOException {
        int offset = size();
        prevStringOffset = offset;
        writeVarInt(s.length());
        writeChars(s);
        return offset;
    }

    @Override
    public int writeVarInt(int i) throws IOException {
        int b;
        b = i & 0x7f;
        i >>>= 7;
        final OutputStream outputStream = out;
        written++;
        if (i == 0) {
            outputStream.write(b);
            return 1;
        }
        out.write(b | 0x80);

        b = i & 0x7f;
        i >>>= 7;
        written++;
        if (i == 0) {
            out.write(b);
            return 2;
        }
        out.write(b | 0x80);

        b = i & 0x7f;
        i >>>= 7;
        written++;
        if (i == 0) {
            out.write(b);
            return 3;
        }
        out.write(b | 0x80);

        b = i & 0x7f;
        i >>>= 7;
        written++;
        if (i == 0) {
            out.write(b);
            return 4;
        }
        out.write(b | 0x80);
        written++;
        out.write(i);
        return 5;
    }

    @Override
    public int writeVarInt(long j) throws IOException {
        int i = ((int) j) & ((1 << 28) - 1); // 0..27
        int i2 = ((int) (j >>> 28)) & 0x7f; // 28..34
        int i3 = (int) (j >>> 35); // 35..63

        int b;
        b = i & 0x7f; // 0..6
        i >>>= 7;
        final OutputStream outputStream = out;
        written++;
        if (i == 0) {
            outputStream.write(b);
            return 1;
        }
        out.write(b | 0x80);

        b = i & 0x7f; // 7..13
        i >>>= 7;
        written++;
        if (i == 0) {
            out.write(b);
            return 2;
        }
        out.write(b | 0x80);

        b = i & 0x7f; // 14..20
        i >>>= 7;
        written++;
        if (i == 0) {
            out.write(b);
            return 3;
        }
        out.write(b | 0x80);

        b = i & 0x7f; // 21..27
        written++;
        if (i2 == 0 && i3 == 0) {
            out.write(b);
            return 4;
        }
        out.write(b | 0x80);

        b = i2;
        written++;
        if (i3 == 0) {
            out.write(b);
            return 5;
        }
        out.write(b | 0x80);

        return 5 + writeVarInt(i3);
    }

    @Override
    public final int writeVarIntZigZag(int src) throws IOException {
        return writeVarInt((src << 1) ^ (src >> 31));
    }

    @Override
    public final int writeVarIntZigZag(long src) throws IOException {
        return writeVarInt((src << 1) ^ (src >> 63));
    }
}
