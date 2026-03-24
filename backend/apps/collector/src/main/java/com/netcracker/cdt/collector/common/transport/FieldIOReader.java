package com.netcracker.cdt.collector.common.transport;

import com.netcracker.common.ProtocolConst;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public final class FieldIOReader {
//    private final Socket socket;
    private final Supplier<Boolean> socketDead;
    private final InputStream in;
    private final ByteBuffer buffer = ByteBuffer.allocate(ProtocolConst.DATA_BUFFER_SIZE);
    private final byte[] array = buffer.array();
    long read = 0;

    public FieldIOReader(Supplier<Boolean> socketDead, InputStream in) {
//        this.socket = socket;
        this.socketDead = socketDead;
        this.in = in;
    }

    //since JDK 9 broke compatibility and ByteBuffer.clear returns different types, need to make code independent of this fact
    private void clearBuffer(){
        ((Buffer)buffer).clear();
    }

    private void readNumBytes(int numBytes) throws IOException {
        int numRead = 0;
        while(numRead != numBytes){
            if(numBytes < numRead) {
                throw new ProfilerProtocolException("Read more than requested. Requested: " + numBytes + ". Read: " + numRead);
            }
            if(Thread.interrupted()){
                throw new ProfilerProtocolException("Interrupted");
            }
            int numJustRead;
            if((numJustRead = in.read(array, numRead, numBytes - numRead)) > 0) {
                numRead += numJustRead;
            } else {
                if (socketDead.get()) {
                    throw new ProfilerProtocolException("Failed to read " + numBytes + " from socket. Only " + numRead + " have been read");
                }
                //park for half a millisecond to wait for more data from socket
                LockSupport.parkNanos(500000L);
            }
        }
        if (Log.isTraceEnabled()) {
            Log.tracef("READ %d/%d bytes: %s", numRead, numBytes, bytesToString(array, 0, numRead));
        }
        read += numRead;
    }

    public UUID UUID() throws IOException {
        long msb = Long();
        long lsb = Long();
        if(msb == 0 && lsb == 0) {
            return null;
        }
        return new UUID(msb, lsb);
    }

    public String String() throws IOException {
        int stringLen = Field();
        return new String(buffer.array(), 0, stringLen, StandardCharsets.UTF_8);
    }

    public int Field() throws IOException {
        clearBuffer();
        //read integer length
        readNumBytes(4);
        int length = buffer.getInt(0);
        if (length > array.length) {
            throw reportError("requested length of field " + length + " exceeds max length of " + array.length);
        }
        readNumBytes(length);

        return length;
    }

    public long Long() throws IOException {
        readNumBytes(8);
        return buffer.getLong(0);
    }

    public int Int() throws IOException {
        readNumBytes(4);
        return buffer.getInt(0);
    }

    public byte Byte() throws IOException {
        readNumBytes(1);
        return buffer.get(0);
    }

    public byte[] getArray() {
        return array;
    }

    public RuntimeException reportError(String message){
        // add details about the stream into a message?
        throw new ProfilerProtocolException(message);
    }

    static String bytesToString(byte[] b, int off, int len) {
        var buf = ByteBuffer.wrap(b, off, len);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        for (int i = buf.position(); i < buf.limit(); i++) {
            pw.format("%02X ", buf.get(i));
        }
        return sw.toString();
    }

}
