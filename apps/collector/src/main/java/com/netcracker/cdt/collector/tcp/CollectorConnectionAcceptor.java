package com.netcracker.cdt.collector.tcp;

import com.netcracker.cdt.collector.common.CollectorConfig;
import com.netcracker.cdt.collector.common.Metrics;
import com.netcracker.cdt.collector.services.PodDumper;
import com.netcracker.cdt.collector.services.StreamDumper;
import com.netcracker.common.Time;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import com.netcracker.common.ProtocolConst;

@ApplicationScoped
@LookupIfProperty(name = "service.type", stringValue = "collector")
public class CollectorConnectionAcceptor {
    @Inject
    Time time;

    @Inject
    StreamDumper streamDumper;
    @Inject
    PodDumper podDumper;
    @Inject
    Metrics metrics;
    @Inject
    CollectorConfig config;
    @Inject
    CollectorOrchestratorThread collectorOrchestratorThread;

    public void start() {
        Log.infof("Preparing the collector accepted thread");
        // CollectorServer.waitInitialized(); // DB
        var t = new Thread() {
            @Override
            public void run() {
                Log.infof("Started the collector accepted thread. Listening to %d port", ProtocolConst.PLAIN_SOCKET_PORT);
                try (ServerSocket server = new ServerSocket(ProtocolConst.PLAIN_SOCKET_PORT, config.getMaxConnections())) {
                    Log.infof("Started listening on socket %d", ProtocolConst.PLAIN_SOCKET_PORT);
                    while (isAlive()) {
                        acceptSocketConnect(server);
                    }
                } catch (Exception e) {
                    Log.errorf(e, "Failed to start listening on port {}", ProtocolConst.PLAIN_SOCKET_PORT);
                }
            }
        };
        t.setName("ConnectionAcceptor");
        t.start();
    }

    private void acceptSocketConnect(ServerSocket server) throws IOException {
        Socket socket = null;
        try {
            socket = server.accept();
            Log.infof("Received connection from %s", socket.getRemoteSocketAddress().toString());

            var pac = new ProfilerAgentConnection(time, streamDumper, podDumper, metrics, socket);
            pac.acceptConnection(collectorOrchestratorThread);

            Log.debugf("Scheduled executor for connection");

        } catch (Exception e) {
            Log.error("Exception when picking up new connection ", e);
            if (socket != null) {
                socket.close();
            }
        }
    }

}
