package com.netcracker.cdt.collector.common.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.function.Supplier;

public class FieldIO {
    public final FieldIOReader read;
    public final FieldIOWriter write;

    public FieldIO(Supplier<Boolean> socketDead, InputStream in, OutputStream out) {
        this.read = new FieldIOReader(socketDead, in);
        this.write = new FieldIOWriter(out);
    }

    public long readBytes() {
        return read.read;
    }

    public long sentBytes() {
        return write.sent;
    }

    public int readField() throws IOException {
        return read.Field();
    }

    public void writeField(byte[] toWrite, int offset, int length) throws IOException {
        write.Field(toWrite, offset, length);
    }

    public String readString() throws IOException {
        return read.String();
    }

    public void writeString(String toWrite) throws  IOException {
        write.String(toWrite);
    }

    public long readLong() throws IOException {
        return read.Long();
    }

    public void writeLong(long toWrite) throws IOException {
        write.Long(toWrite);
    }

    public int readInt() throws IOException {
        return read.Int();
    }

    public void writeInt(int toWrite) throws IOException {
        write.Int(toWrite);
    }

    public int readByte() throws IOException {
        return read.Byte();
    }

    public void writeByte(byte toWrite) throws IOException {
        write.Byte(toWrite);
    }

    public UUID readUUID() throws IOException {
        return read.UUID();
    }

    public void writeUUID(UUID toWrite) throws IOException {
        write.UUID(toWrite);
    }

    public void writeCommand(int commandId) throws IOException {
        write.Command(commandId);
    }

    public byte[] getArray() {
        return read.getArray();
    }
}
