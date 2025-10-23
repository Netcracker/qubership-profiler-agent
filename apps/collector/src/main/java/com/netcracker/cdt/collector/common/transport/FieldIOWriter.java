package com.netcracker.cdt.collector.common.transport;

import com.netcracker.common.ProtocolConst;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class FieldIOWriter {
    private final OutputStream out;
    private final ByteBuffer buffer = ByteBuffer.allocate(ProtocolConst.DATA_BUFFER_SIZE);
    private final byte[] array = buffer.array();
    long sent;

    public FieldIOWriter(OutputStream out) {
        this.out = out;
    }

    //since JDK 9 broke compatibility and ByteBuffer.clear returns different types, need to make code independent of this fact
    private void clearBuffer(){
        ((Buffer)buffer).clear();
    }

    public void UUID(UUID toWrite) throws IOException {
        if(toWrite == null) {
            Long(0);
            Long(0);
        } else {
            long msb = toWrite.getMostSignificantBits();
            long lsb = toWrite.getLeastSignificantBits();
            Long(msb);
            Long(lsb);
        }
    }

    public void String(String toWrite) throws  IOException {
        byte[] bytes = toWrite.getBytes(StandardCharsets.UTF_8);
        Field(bytes, 0, bytes.length);
    }

    public void Field(byte[] toWrite, int offset, int length) throws IOException {
        buffer.putInt(0, length);
        out.write(buffer.array(), 0, 4);
        out.write(toWrite, offset, length);
        sent += length + 4;
    }

    public void Long(long toWrite) throws IOException {
        clearBuffer();
        buffer.putLong(toWrite);
        out.write(array, 0, 8);
        sent += 8;
    }

    public void Int(int toWrite) throws IOException {
        clearBuffer();
        buffer.putInt(toWrite);
        out.write(array, 0, 4);
        sent += 4;
    }

    public void Byte(byte toWrite) throws IOException {
        clearBuffer();
        buffer.put(toWrite);
        out.write(array, 0, 1);
        sent += 1;
    }

    public void Command(int commandId) throws IOException {
        out.write(commandId);
        sent += 1;
    }

}
