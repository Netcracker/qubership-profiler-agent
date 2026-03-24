package com.netcracker.cdt.collector.common.transport;

import com.netcracker.common.ProtocolConst;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

public class EndlessSocketInputStream  extends FilterInputStream {
    public static final long PARK_TIME_NANOS = 500000L;

    public EndlessSocketInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        int result;
        long callStarted = System.currentTimeMillis();
        while((result = super.read()) == -1){
            LockSupport.parkNanos(PARK_TIME_NANOS);
            checkFailed(callStarted);
        }
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int result;
        long callStarted = System.currentTimeMillis();
        while((result = super.read(b)) == -1){
            LockSupport.parkNanos(PARK_TIME_NANOS);
            checkFailed(callStarted);
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result;
        long callStarted = System.currentTimeMillis();
        while((result = super.read(b, off, len)) == -1){
            LockSupport.parkNanos(PARK_TIME_NANOS);
            checkFailed(callStarted);
        }
        return result;
    }

    private void checkFailed(long callStarted) {
        if (Thread.interrupted()){
            throw new ProfilerProtocolException("thread has been interrupted");
        }
        if (System.currentTimeMillis() - callStarted > ProtocolConst.PLAIN_SOCKET_READ_TIMEOUT) {
            throw new ProfilerProtocolTimeoutException("Timeout while waiting for a response from socket");
        }
    }
}
