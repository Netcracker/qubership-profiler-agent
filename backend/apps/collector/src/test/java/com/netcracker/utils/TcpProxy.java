package com.netcracker.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class TcpProxy {
    private final int localPort;
    private final String remoteHost;
    private final int remotePort;
    private final List<Thread> threads = new ArrayList();

    public TcpProxy(int localPort, String remoteHost, int remotePort) {
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    public void start() {
        threads.add(Thread.startVirtualThread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(localPort);
                while (true) {
                    Socket socket = serverSocket.accept();
                    threads.add(Thread.startVirtualThread(new Connection(socket, remoteHost, remotePort)));
                    Thread.sleep(10);
                }
            } catch (Exception ignored) {
            }
        }));
    }

    public void stop() {
        for (Thread t: threads) {
            if (t.isAlive()) {
                t.interrupt();
            }
        }
    }

    static class Connection implements Runnable {
        private final Socket clientsocket;
        private final String remoteIp;
        private final int remotePort;
        private Socket serverConnection = null;

        public Connection(Socket clientsocket, String remoteIp, int remotePort) {
            this.clientsocket = clientsocket;
            this.remoteIp = remoteIp;
            this.remotePort = remotePort;
        }

        @Override
        public void run() {
            try {
                serverConnection = new Socket(remoteIp, remotePort);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            Thread.startVirtualThread(new Proxy(clientsocket, serverConnection));
            Thread.startVirtualThread(new Proxy(serverConnection, clientsocket));
            Thread.startVirtualThread(() -> {
                while (true) {
                    if (clientsocket.isClosed()) {
                        closeServerConnection();
                        break;
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
            });
        }

        private void closeServerConnection() {
            if (serverConnection != null && !serverConnection.isClosed()) {
                try {
                    serverConnection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    static class Proxy implements Runnable {
        private final Socket in;
        private final Socket out;

        public Proxy(Socket in, Socket out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                InputStream is = in.getInputStream();
                OutputStream os = out.getOutputStream();
                if (is == null || os == null) {
                    return;
                }

                byte[] reply = new byte[4096];
                int bytesRead;
                while (-1 != (bytesRead = is.read(reply))) {
                    os.write(reply, 0, bytesRead);
                }
            } catch (SocketException ignored) {
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
