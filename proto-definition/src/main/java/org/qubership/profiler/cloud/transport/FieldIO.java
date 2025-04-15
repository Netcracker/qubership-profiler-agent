package org.qubership.profiler.cloud.transport;

import static org.qubership.profiler.cloud.transport.ESCStopWatch.getWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

public class FieldIO {
    boolean traceMode = false;

    public static ThreadLocal<ESCStopWatch> stopWatch = new ThreadLocal<>();

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private WatchDogCallback watchDog;
    private boolean currentCallWrites = false;
    private ByteBuffer buffer = ByteBuffer.allocate(ProtocolConst.DATA_BUFFER_SIZE);
    private byte[] array = buffer.array();

    public FieldIO(Socket socket, InputStream in, OutputStream out) {
        this.socket = socket;
        this.in = in;
        this.out = out;
    }

    public FieldIO(Socket socket, InputStream in, OutputStream out, org.qubership.profiler.cloud.transport.WatchDogCallback watchDog) {
        this(socket, in, out);
        this.watchDog = watchDog;
    }

    private void startCall(boolean write){
        getWatch(stopWatch).start();
        currentCallWrites =write;
    }

    private void endCall(){
        getWatch(stopWatch).stop();
        if(!currentCallWrites && watchDog != null) {
            watchDog.accessed();
        }
    }

    //since JDK 9 broke compatibility and ByteBuffer.clear returns different types, need to make code independent of this fact
    private void clearBuffer(){
        ((Buffer)buffer).clear();
    }

    private void readNumBytes(int numBytes) throws IOException {
        startCall(false);
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
                if(socket.isClosed() || socket.isInputShutdown() || !socket.isConnected() || !socket.isBound()) {
                    throw new org.qubership.profiler.cloud.transport.ProfilerProtocolException("Failed to read " + numBytes + " from socket. Only " + numRead + " have been read");
                }
                //park for half a millisecond to wait for more data from socket
                LockSupport.parkNanos(500000L);
            }
        }
        endCall();
    }

    public void writeField(byte[] toWrite, int offset, int lenght) throws IOException {
        startCall(true);
        if(traceMode){
            System.out.println("Writing field. Length is " + lenght);
        }
        buffer.putInt(0, lenght);
        out.write(buffer.array(), 0, 4);
        out.write(toWrite, offset, lenght);
        endCall();
    }

    public int readField() throws IOException {
        startCall(false);
        clearBuffer();
        //read integer length
        readNumBytes(4);
        int lenght = buffer.getInt(0);
        if (lenght > array.length) {
            throw reportError("requested length of field " + lenght + " exceeds max lenght of " + array.length);
        }
        readNumBytes(lenght);

        if(traceMode){
            System.out.println("Reading field. Length: " + lenght);
        }
        endCall();

        return lenght;
    }

    public String readString() throws IOException {
        int stringLenght = readField();
        return new String(buffer.array(), 0, stringLenght, StandardCharsets.UTF_8);
    }

    public void writeString(String toWrite) throws  IOException {
        byte[] bytes = toWrite.getBytes(StandardCharsets.UTF_8);
        writeField(bytes, 0, bytes.length);
    }

    public long readLong() throws IOException {
        readNumBytes(8);
        long result = buffer.getLong(0);
        if(traceMode){
            System.out.println("Read long " + result);
        }
        return result;
    }

    public int readInt() throws IOException {
        readNumBytes(4);
        int result = buffer.getInt(0);
        if(traceMode){
            System.out.println("Read int " + result);
        }
        return result;
    }

    public void writeLong(long toWrite) throws IOException {
        startCall(true);
        clearBuffer();
        buffer.putLong(toWrite);
        out.write(array, 0, 8);
        if(traceMode){
            System.out.println("Written long " + toWrite);
        }
        endCall();
    }

    public void writeInt(int toWrite) throws IOException {
        startCall(true);
        clearBuffer();
        buffer.putInt(toWrite);
        out.write(array, 0, 4);
        if(traceMode){
            System.out.println("Written int " + toWrite);
        }
        endCall();
    }

    public UUID readUUID() throws IOException {
        long msb = readLong();
        long lsb = readLong();
        if(msb == 0 && lsb == 0) {
            return null;
        }
        return new UUID(msb, lsb);
    }

    public void writeUUID(UUID toWrite) throws IOException {
        if(toWrite == null) {
            writeLong(0);
            writeLong(0);
        } else {
            long msb = toWrite.getMostSignificantBits();
            long lsb = toWrite.getLeastSignificantBits();
            writeLong(msb);
            writeLong(lsb);
        }
    }

    public byte[] getArray() {
        return array;
    }

    public void writeCommand(int commandId) throws IOException {
        startCall(true);
        out.write(commandId);
        endCall();
    }

    public RuntimeException reportError(String message){
        //todo: add details about the stream into a message
        throw new ProfilerProtocolException(message);
    }
}
