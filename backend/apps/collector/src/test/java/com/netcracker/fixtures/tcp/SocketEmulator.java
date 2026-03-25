package com.netcracker.fixtures.tcp;

import com.netcracker.cdt.collector.common.transport.FieldIO;
import com.netcracker.cdt.collector.common.transport.FieldIOReader;
import com.netcracker.cdt.collector.common.transport.FieldIOWriter;
import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.cdt.collector.tcp.AgentConnection;
import com.netcracker.cdt.collector.tcp.ProfilerAgentReader;
import com.netcracker.common.Time;

import java.io.*;

import static com.netcracker.common.ProtocolConst.DATA_BUFFER_SIZE;

public class SocketEmulator {
    private final AgentConnection mock;
    private final InputStream serverIn;
    private final OutputStream serverOut;
    private final FieldIO client;
    private boolean socketDead;

    public final FieldIOReader read;
    public final FieldIOWriter write;
    private ProfilerAgentReader reader;

    public SocketEmulator() throws IOException {
        this("test");
    }

    public SocketEmulator(String podName) throws IOException {
        serverIn = new PipedInputStream(DATA_BUFFER_SIZE + 30); // +30 for non-parallel tests
        serverOut = new PipedOutputStream();

        var clientOut = new PipedOutputStream((PipedInputStream) serverIn);
        var clientIn = new PipedInputStream((PipedOutputStream) serverOut);

        this.socketDead = false;
        client = new FieldIO(() -> socketDead, clientIn, clientOut);
        read = client.read;
        write = client.write;

//        read.setDebug(true);

        this.mock = new AgentConnection() {
            @Override
            public boolean commandAvailable() throws IOException {
                return !isSocketDead() && client.sentBytes() > client.readBytes();
            }

            @Override
            public boolean isSocketDead() {
                return socketDead;
            }

            @Override
            public String getConnectionName() {
                return podName;
            }

            @Override
            public void close(String reason) {
                done();
            }
        };
    }

    public ProfilerAgentReader createAgentReader(Time time, StreamDumper streamDumper, PodDumper podDumper) {
        reader = new ProfilerAgentReader(time, streamDumper, podDumper, null, "socket emulator", mock, serverIn, serverOut);
        return reader;
    }

    public void done() {
        try {
            socketDead = true;
            serverOut.close();
            serverIn.close();
            if (reader != null) {
                reader.closeStreams();
            }
        } catch (Exception ignored) {

        }
    }
}
