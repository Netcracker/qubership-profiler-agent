package com.netcracker.cdt.collector.tcp;

import com.netcracker.cdt.collector.common.Metrics;
import com.netcracker.cdt.collector.common.transport.EndlessSocketInputStream;
import com.netcracker.cdt.collector.common.transport.ProfilerProtocolTimeoutException;
import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.Time;
import com.netcracker.common.models.pod.PodStatus;
import io.quarkus.logging.Log;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.netcracker.common.ProtocolConst.*;

public class ProfilerAgentConnection implements AgentConnection {
    private final Time time;
    private final StreamDumper streamDumper;
    private final PodDumper podDumper;
    private final Metrics metrics;

    private Socket socket;

    private ProfilerAgentReader reader;
    private AtomicBoolean forcedClose = new AtomicBoolean(false);

    public ProfilerAgentConnection(Time time, StreamDumper streamDumper, PodDumper podDumper, Metrics metrics, Socket socket) {
        this.time = time;
        this.streamDumper = streamDumper;
        this.podDumper = podDumper;
        this.metrics = metrics;
        this.socket = socket;
    }

    public void acceptConnection(CollectorOrchestratorThread orchestrator) {
        Thread.ofVirtual().start(() -> {
            try {
                Thread.currentThread().setName("agent-%d".formatted(socket.getPort()));
                Log.debugf("Separate thread for %s", socket.toString());
                initIfNecessary();
                register(orchestrator);
                run();
            } catch (Exception e) {
                if (reader != null) {
                    close("Can't connect");
                    Log.errorf(e, "POD %s failed to connect ", reader.podId());
                } else {
                    Log.errorf(e, "Error during initialization");
                }
            }
        });
        Log.infof("A connection agent was scheduled for %s", socket.toString());
    }

    private void initIfNecessary() throws IOException {
        if (reader != null) {
            return;
        }
        socket.setKeepAlive(true);
        socket.setSoTimeout(PLAIN_SOCKET_READ_TIMEOUT);
        socket.setReceiveBufferSize(PLAIN_SOCKET_SND_BUFFER_SIZE);
        socket.setSendBufferSize(PLAIN_SOCKET_RCV_BUFFER_SIZE);

        var is = new EndlessSocketInputStream(socket.getInputStream());
        var os = new BufferedOutputStream(socket.getOutputStream(), DATA_BUFFER_SIZE);
        reader = new ProfilerAgentReader(time, streamDumper, podDumper, metrics,
                socket.getRemoteSocketAddress().toString(), this,
                is, os);
        reader.flushCompressor(true); // Because input stream on the other side needs to initialize off of non-zero input

        Log.debugf("Waiting for first action from %s", socket.toString());
        // should retrieve data about pod
        reader.firstAction();
    }

    private void register(CollectorOrchestratorThread orchestrator) {
        Log.infof("Register thread for %s as %s", socket, reader.podId());
        Thread.currentThread().setName("agent-" + socket.getPort() + "-" + reader.podId());
        orchestrator.addConnection(reader.pod(), this);
    }

    private void run() {
        try {
            Log.debugf("[%s] run while loop", getConnectionName());
            while (!reader.state.isDead()) {
                if (reader.state.readyForCommand()) {
                    reader.nextAction();
                } else {
                    Thread.sleep(10);
                }

                if (reader.isSocketDead()) {
                    Log.warnf("[%s] socket dead", getConnectionName());
                    break;
                }
            }
            Log.debugf("[%s] finish loop", getConnectionName());
        } catch (ProfilerProtocolTimeoutException | SocketTimeoutException e) {
            Log.errorf("[%s] Client dropped by timeout", getConnectionName());
            reader.shutdownIfOpen();
        } catch (SocketException e) {
            Log.errorf("[%s] Socket error: %s", getConnectionName(), e.getMessage());
            reader.shutdownIfOpen();
        } catch (Exception e) {
            if (!forcedClose.get()) {
                Log.errorf(e, "[%s] Exception when receiving data. Will close the socket", getConnectionName());
            } else {
                Log.infof("[%s] Forced connection closing (may be got new one from pod)", getConnectionName());
            }
            reader.shutdownIfOpen();
        } finally {
            reader.done();
            reader.closeStreams();
            close("finish loop");
        }
    }

    public boolean isDead() { // for orchestrator
        return reader.state.isDead() || isSocketDead();
    }

    @Override
    public boolean isSocketDead() {
        return socket == null || socket.isClosed() || !socket.isConnected() || socket.isInputShutdown() || !socket.isBound();
    }

    boolean timeToKill() {
        return reader.state.timeToKill();
    }

    public String getConnectionName() {
        if (reader != null) {
            return reader.state.toString();
        }
        if (socket != null) {
            return socket.toString();
        }
        return "unknown";
    }

    PodStatus getPod() {
        return reader.state.pod;
    }

    public boolean shutdownComplete() {
        return reader.state.shutdownComplete();
    }

    public void kill() {
        close("kill from orchestrator");
//        return reader.kill();
    }

    @Override
    public void close(String reason) {
        // Socket may be reset by a previous shutdown.
        // Shutdown may be requested by an orchestrator thread during another close
        if (reader.state.shutdownComplete() || socket == null) {
            return;
        }
        Log.infof("[%s] Closing connection. Reason: %s. Idle for %d ms", getConnectionName(), reason, reader.state.idleMs());
        try {
            forcedClose.set(true);
            socket.close();
            socket = null;
        } catch (IOException e) {
            Log.error("[%s] Can't close the socket", getConnectionName(), e);
        }
        reader.closeStreams();
    }

    @Override
    public boolean commandAvailable() throws IOException {
        if (isSocketDead()) {
            return false;
        }
        boolean res = reader.input == null
                || (!socket.isClosed() && socket.getInputStream().available() > 0)
                || reader.input.available() > 0;
        return res;
    }

}
