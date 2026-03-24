package com.netcracker.cdt.collector.tcp;

import com.netcracker.cdt.collector.common.Metrics;
import com.netcracker.cdt.collector.common.StreamNotInitializedException;
import com.netcracker.cdt.collector.common.models.StreamRotatedInfo;
import com.netcracker.cdt.collector.common.transport.FieldIO;
import com.netcracker.cdt.collector.common.transport.ProfilerProtocolException;
import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.Time;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodStatus;
import io.quarkus.logging.Log;

import java.io.*;
import java.util.*;

import static com.netcracker.common.ProtocolConst.*;

public class ProfilerAgentReader {
    private final Time time;
    private final StreamDumper streamDumper;
    private final PodDumper podDumper;
    private final Metrics metrics;

    private final AgentConnection connection;
    private final FieldIO io;
    final InputStream input;
    final OutputStream output;

    final ConnectionState state;

    private final Set<UUID> servedStreams = new HashSet<>();

    public ProfilerAgentReader(Time time, StreamDumper streamDumper, PodDumper podDumper, Metrics metrics,
                               String name,
                               AgentConnection connection,
                               InputStream input, OutputStream output) {
        this.time = time;
        this.streamDumper = streamDumper;
        this.podDumper = podDumper;
        this.metrics = metrics;
        this.input = input;
        this.output = output;
        this.connection = connection;
        this.io = new FieldIO(connection::isSocketDead, input, output);

        this.state = new ConnectionState(time, this, name);
    }

    void firstAction() throws IOException {
        Log.debugf("[%s] Processing first command", connection.getConnectionName());
        processCommand();
        // The first command is always init stream, and it gives the name of the pod
        if (state.pod.isEmpty()) {
            // If the first command was the old GET_PROTOCOL_VERSION, the next init stream command should always pass the pod name
            Log.warnf("[%s] Older client? Have to process second command for initialization", connection.getConnectionName());
            processCommand();
        }
        state.processed(true);
    }

    boolean nextAction() throws IOException {
//        Log.debugf("[%s] Ready for next action", connection.getConnectionName());
        if (Thread.interrupted() || state.needsShutdown()) {
            connection.close("shutdown");
            return false;
        }

        boolean processed = false;
        if (commandAvailable()) {
            processCommand();
            processed = true;
        }

        if (state.needsFlushCheck()) {
            flushIfNecessary();
        }
        return processed;
    }

    void processCommand() throws IOException {
        Log.tracef("[%s] Looking for command", connection.getConnectionName());
        byte commandId = (byte) input.read();
        state.accessed();
        Log.tracef("[%s] Received command %s", connection.getConnectionName(), commandId);
        try {
            switch (commandId) {
                case -1:
                    throw new ProfilerProtocolException("End of input!");

                case COMMAND_INIT_STREAM: // deprecated
                    // req
                    state.setNamespace(io.readString());
                    state.setMicroservice(io.readString());
                    state.setPodName(io.readString());

                case COMMAND_INIT_STREAM_V2:
                    // req
                    String streamName = io.readString();
                    int requestedRollingSequenceId = io.readInt();
                    int resetRequired = io.readInt();

                    var stream = StreamType.byName(streamName);
                    if (stream == null) {
                        Log.errorf("[%s] Invalid stream type: %s", connection.getConnectionName(), streamName);
                        io.writeUUID(null);
                        flushCompressor(true);
                        break;
                    }

                    // action
                    long rotationPeriod  = streamDumper.getRotationPeriod(stream);
                    long requiredRotationSize = streamDumper.getRequiredRotationSize(stream);
                    var streamRequest = state.pod.newStreamRequest(time.now(), stream,
                            requestedRollingSequenceId, resetRequired > 0, false);

                    try {
                        StreamRotatedInfo update = streamDumper.streamOpened(streamRequest);
                        this.servedStreams.removeAll(update.cleanedUpStreamIDs());

                        // resp
                        io.writeUUID(update.newStreamId());
                        io.writeLong(rotationPeriod);
                        io.writeLong(requiredRotationSize);
                        io.writeInt(update.rollingSequenceId());
                    } catch (Exception e) {
                        Log.errorf(e, "[%s] Exception when initializing stream %s", connection.getConnectionName(), streamRequest);
                        io.writeUUID(null);
                        flushCompressor(true);
                        break;
                    }
                    flushCompressor(true);
//                    MetricsController.setCommandInitStreamExecutionTime(namespaceName, microserviceName, podName, startCommandStopWatch.getAndReset());
//                    MetricsController.setAgentUptime(namespaceName, microserviceName, podName, agentUptime);
                    break;

                case COMMAND_RCV_DATA:
                    // req
                    UUID handleId = io.readUUID();
                    int contentLength = io.readField();
                    // action
                    try {
                        streamDumper.receiveData(handleId, io.getArray(), 0, contentLength);
                        servedStreams.add(handleId);
                        sendCommands(false);
                    } catch (StreamNotInitializedException e) {
                        Log.infof("[%s] Stream '%s' is not registered. Requesting stream rotation", connection.getConnectionName(),  e.getUuid());
                        output.write(ACK_ERROR_MAGIC);
                        connection.close("unregistered stream");
                    } catch (Exception e) {
                        Log.errorf(e, "[%s] Exception when receiving data. of length %d from stream %s of pod %s. triggering stream rotation",
                                connection.getConnectionName(), contentLength, handleId, state.pod);
                        output.write(ACK_ERROR_MAGIC);
                        connection.close("receiving error");
                    }
                    flushCompressor(false);
                    if (metrics != null) {
                        metrics.setReceiveFromAgentBytes(state.pod.namespace(), state.pod.service(), state.pod.podName(), contentLength);
//                    MetricsController.setCommandRcvDataExecutionTime(this.namespaceName, this.microserviceName, this.podName, startCommandStopWatch.getAndReset());
//                    MetricsController.setReceiveFromAgentBytes(this.namespaceName, this.microserviceName, this.podName, contentLength);
                    }
                    break;

                case COMMAND_CLOSE:
                    connection.close("requested");
                    break;

                case COMMAND_GET_PROTOCOL_VERSION: // deprecated
                    // resp
                    io.writeLong(PROTOCOL_VERSION);
                    flushCompressor(true);
                    break;

                case COMMAND_GET_PROTOCOL_VERSION_V2:
                    // req
                    state.setClientProtocolVersion(io.readLong());
                    state.setPodName(io.readString());
                    state.setMicroservice(io.readString());
                    state.setNamespace(io.readString());
                    // action: clean up old pod
                    try {
                        var oldStreams = streamDumper.cleanupPodStreams(state.pod.podId());
                        if (oldStreams.size() > 0) {
                            this.servedStreams.removeAll(oldStreams);
                        }
                    } catch (Exception e) {
                        Log.errorf(e, "[%s] Exception when cleaning old pod stream %s",
                                connection.getConnectionName(), state.pod.podId());
                    }
                    // action: register new pod
                    state.pod.touch(time.now());
                    podDumper.initPod(state.pod); // TODO check: should finish previous pod?
                    // resp
                    io.writeLong(PROTOCOL_VERSION_V2);
                    flushCompressor(true);
                    break;

                case COMMAND_REQUEST_ACK_FLUSH:
                    // action
                    state.pod.touch(time.now());
                    sendCommands(true);
                    break;

                case COMMAND_REPORT_COMMAND_RESULT:
                    // req
                    UUID executedCommandId = io.readUUID();
                    int success = input.read();
                    reportCommandStatus(executedCommandId, COMMAND_SUCCESS == success);
                    break;
                default:
                    throw new ProfilerProtocolException("Unknown command " + commandId + " received from " + state);
            }
        } finally {
            Log.tracef("[%s] Processed command", connection.getConnectionName(), commandId);
        }

    }

    void flushCompressor(boolean force) throws IOException {
        if (force || state.needFlush()) {
            output.flush();
            state.flush();
        }
    }

    private void flushIfNecessary() {
        streamDumper.flushStreams(servedStreams);
        state.flush();
    }

    void shutdownIfOpen() {
        if (state.shutdownComplete()) {
            return;
        }
        state.requestShutdown();
    }

    private void sendCommands(boolean flush) throws IOException {
        io.write.Byte((byte) 0);
        if (flush) {
            flushCompressor(true);
        }
    }

    private void reportCommandStatus(UUID executedCommandId, boolean success) {
//        streamFacade.reportCommandSuccess(podName, executedCommandId, success);
        Log.warnf("[deprecated] Report command status for %s: id %s, success? %b",
                state.pod, executedCommandId.toString(), success);
    }

    public void done() {
        state.processed(false);
    }

    public void closeStreams() {
        for (UUID served : servedStreams) {
            try {
                streamDumper.closeAndForget(served);
            } catch (Exception e) {
                Log.error("Can not close and forget stream " + served + " of " + state.pod, e);
            }
        }
    }

    public PodStatus pod() {
        return state.pod;
    }

    public String podName() {
        return state.pod.podId();
    }

    public String podId() {
        return state.pod.podId();
    }

    boolean commandAvailable() throws IOException {
        return connection.commandAvailable();
    }

    boolean isSocketDead() {
        return connection.isSocketDead();
    }

}
