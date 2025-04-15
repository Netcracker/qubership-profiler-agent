package org.qubership.profiler.client;

import org.qubership.profiler.agent.DumperCollectorClient;
import org.qubership.profiler.agent.DumperRemoteControlledStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class RollingChunkStream  extends OutputStream implements DumperRemoteControlledStream {
    private static final Logger log = LoggerFactory.getLogger(RollingChunkStream.class);

    private final int rollingSequenceId;
    private final String streamName;
    private final long rotationPeriod;
    private final long requiredRotationSize;
    private final DumperCollectorClient collectorClient;

    public RollingChunkStream(final int rollingSequenceId,
                              final String streamName,
                              final long rotationPeriod,
                              final long requiredRotationSize,
                              final DumperCollectorClient collectorClient) {
        this.rollingSequenceId = rollingSequenceId;
        this.streamName = streamName;
        this.rotationPeriod = rotationPeriod;
        this.requiredRotationSize = requiredRotationSize;
        this.collectorClient = collectorClient;
    }

    public String getStreamName() {
        return streamName;
    }

    public long getRotationPeriod() {
        return rotationPeriod;
    }

    public long getRequiredRotationSize() {
        return requiredRotationSize;
    }


    public int getRollingSequenceId() {
        return rollingSequenceId;
    }

    public void write( byte[] bytes, int offset, int length) throws IOException {
        collectorClient.write(bytes, offset, length, getStreamName());
    }

    @Override
    public void write(int word) throws IOException { // implemented for compatibility, not expected to be used
        write(new byte[]{(byte) word});
    }

    @Override
    public void write( byte[] bytes) throws IOException { // implemented for compatibility, not expected to be used
        this.write(bytes, 0, bytes.length);
    }

    @Override
    public void close() throws IOException {
        //no need to close the collector when rolling chunk stream is rotated.
        //also this one does not hold any resources, so no closing is needed
        //other side will close the stream when next sequence is initialized or when timeout happens
//        collectorClient.shutdown();
    }

    @Override
    public void flush() throws IOException {
        collectorClient.flush();
    }

    @Override
    public OutputStream getOutputStream() {
        return this;
    }

    public DumperCollectorClient getClient() {
        return collectorClient;
    }
}
