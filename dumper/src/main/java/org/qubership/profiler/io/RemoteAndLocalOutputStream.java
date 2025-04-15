package org.qubership.profiler.io;

import org.qubership.profiler.agent.DumperCollectorClient;
import org.qubership.profiler.agent.DumperRemoteControlledStream;
import org.qubership.profiler.cloud.transport.PhraseOutputStream;
import org.qubership.profiler.cloud.transport.ProtocolConst;
import org.qubership.profiler.exception.ProfilerAgentIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * this one writes to both remote stream and local file
 */
public class RemoteAndLocalOutputStream extends OutputStream {
    public static final Logger log = LoggerFactory.getLogger(RemoteAndLocalOutputStream.class);
    private final DumperCollectorClient client;
    private final String streamName;
    private OutputStream local;
    private OutputStream remoteBuffer;
    private int rollingSequenceId = -1;
    private boolean remoteEnabled = true;
    private boolean localEnabled;
    private long rotationPeriod;
    private long requiredRotationSize;
    private boolean resetRequired;

    public RemoteAndLocalOutputStream(final DumperCollectorClient client,
                                      final String streamName,
                                      final int requestedRollingSequenceId,
                                      final boolean localEnabled,
                                      final boolean resetRequired) throws IOException {
        if (client == null) {
            throw new NullPointerException("RemoteAndLocalOutputStream cannot be constructed without CollectorClient");
        }
        this.localEnabled = localEnabled;
        this.client = client;
        this.streamName = streamName;
        this.resetRequired = resetRequired;
        this.remoteBuffer = initRemoteBuffer(requestedRollingSequenceId);
    }

    private OutputStream initRemoteBuffer(int requestedRollingSequenceId) throws IOException {
        DumperRemoteControlledStream chunkStream = client.createRollingChunk(streamName, requestedRollingSequenceId, resetRequired);
        //override the rolling sequence id based on the remote version
        rollingSequenceId = chunkStream.getRollingSequenceId();
        this.rotationPeriod = chunkStream.getRotationPeriod();
        this.requiredRotationSize = chunkStream.getRequiredRotationSize();

        OutputStream remote;

        if ("dictionary".equals(streamName) || "suspend".equals(streamName) || "params".equals(streamName)) {
            remote = new PhraseOutputStream(chunkStream.getOutputStream(), ProtocolConst.MAX_PHRASE_SIZE, ProtocolConst.DATA_BUFFER_SIZE);
        }else {
            remote = new BufferedOutputStream(chunkStream.getOutputStream(), ProtocolConst.DATA_BUFFER_SIZE);
        }

        return remote;
    }


    private OutputStream getLocal() {
        return this.local;
    }

    public void setLocal(OutputStream local) {
        this.local = local;
    }

    private OutputStream getRemote() {
        return remoteBuffer;
    }

    private void checkLocalInitialized() throws ProfilerAgentIOException {
        if (localEnabled && local == null) {
            throw new ProfilerAgentIOException("Local dump is requested but no local stream provided. Stream " + streamName);
        }
    }

    public void writePhrase() throws IOException {
        if (remoteEnabled ) {
            if(remoteBuffer instanceof PhraseOutputStream){
                ((PhraseOutputStream) remoteBuffer).writePhrase();
            }
        }
    }

    @Override
    public void write(int word) throws IOException {
        checkLocalInitialized();
        try {
            if (localEnabled) {
                getLocal().write(word);
            }
        } finally {
            if (remoteEnabled) {
                if(log.isTraceEnabled()) {
                    log.trace("writing 1 byte to remote");
                }
                getRemote().write(word);
            } else {
                log.warn("remote disabled. not writing byte");
            }
        }
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        checkLocalInitialized();
        try {
            if (localEnabled) {
                getLocal().write(bytes);
            }
        } finally { // we want to send data event if local data write failed
            if (remoteEnabled) {
                // remote should be disabled after reinit until next iteration starts to prevent partial data sent
                if(log.isTraceEnabled()) {
                    log.trace("writing {} bytes to remote", bytes.length);
                }
                getRemote().write(bytes);
            } else {
                log.warn("remote disabled. not writing {} bytes", bytes.length);
            }
        }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        checkLocalInitialized();
        try {
            if (localEnabled) {
                getLocal().write(bytes, offset, length);
            }
        } finally { // we want to send data even if local data write failed
            if (remoteEnabled) {
                // remote should be disabled after reinit until next iteration starts to prevent partial data sent
                if(log.isTraceEnabled()) {
                    log.trace("writing {} bytes to remote", length);
                }
                getRemote().write(bytes, offset, length);
            } else {
                log.warn("remote disabled. not writing {} bytes", length);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        checkLocalInitialized();
        try {
            if (localEnabled) {
                getLocal().flush();
            }
        } finally { // we want to flush even if local flush failed
            if (remoteEnabled) {
                // remote should be disabled after reinit until next iteration starts to prevent partial data sent
                if(log.isTraceEnabled()) {
                    log.trace("flushing remote");
                }
                if(client.isOnline()) {
                    getRemote().flush();
                } else {
                    log.debug("not attempting to flush remote output stream {}:{}", streamName, rollingSequenceId);
                }
            } else {
                log.warn("remote disabled. not flushing remote");
            }
        }
    }

    @Override
    public void close() throws IOException {
        checkLocalInitialized();
        try {
            if (localEnabled) {
                try {
                    getLocal().flush();
                } finally {
                    getLocal().close();
                }
            }
        } finally { // we want to close even if local close failed
            if (remoteEnabled) {
                // remote should be disabled after reinit until next iteration starts to prevent partial data sent
                //closing implicitly flushes remote inside FilterOutputStream
                if(log.isTraceEnabled()){
                    log.trace("closing remote");
                }
                if(client.isOnline()) {
                    getRemote().close();
                } else {
                    log.debug("not attempting to close remote output stream {}:{}", streamName, rollingSequenceId);
                }
            } else {
                log.debug("remote disabled. not closing {}:{}", streamName, rollingSequenceId);
            }
        }
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
}
