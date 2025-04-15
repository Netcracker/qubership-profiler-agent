package org.qubership.profiler.transfer;

import org.qubership.profiler.agent.NetworkExportParams;
import org.qubership.profiler.agent.ProfilerData;
import org.qubership.profiler.agent.TimerCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class DataSender extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DataSender.class);

    private ArrayBlockingQueue<ByteArrayOutputStream> jsonsToSend;
    private ArrayBlockingQueue<ByteArrayOutputStream> emptyJsonBuffers;

    private Socket socket;
    private BufferedOutputStream out;
    private String host;
    private int port;
    private int socketTimeout;

    int sleepInterval = 1000;
    long timestamp1 = TimerCache.now;
    private volatile boolean reconfigureRequired;

    private Lock configurationLock = new ReentrantLock();
    private volatile boolean shutdownRequested;
    private boolean forceShutdown;

    final Thread SHUTDOWN_HOOK = new Thread() {
        @Override
        public void run() {
            shutdown();
        }
    };

    public void initalizeConnection() {
        while(true) {
            try {
                closeConnection();
                if(forceShutdown) {
                    return;
                }

                configurationLock.lock();
                try{
                    socket = new Socket(host, port);
                    socket.setSoTimeout(socketTimeout);
                } finally {
                    configurationLock.unlock();
                }

                out = new BufferedOutputStream(socket.getOutputStream());
                log.info("Socket connection initalized");
                reconfigureRequired = false;
                return;
            } catch (IOException e) {
                if(shutdownRequested) {
                    forceShutdown = true;
                }
                try {
                    Thread.sleep(sleepInterval);
                    if (sleepInterval < 60000) {
                        sleepInterval = sleepInterval * 2;
                    }
                } catch (InterruptedException e1) {
                    log.error("Thread interrupted", e1);
                }
                log.error("Can not initalize socket connection: host {}, port {}. Check the server(logstash) and fields call-export->network->host/port in profiler config",host, port);
            }
        }
    }

    public DataSender(NetworkExportParams params) {
        configure(params);
        jsonsToSend = new ArrayBlockingQueue<ByteArrayOutputStream>(ProfilerData.DATA_SENDER_QUEUE_SIZE);
        emptyJsonBuffers = new ArrayBlockingQueue<ByteArrayOutputStream>(ProfilerData.DATA_SENDER_QUEUE_SIZE);
        for (int i = 0; i < ProfilerData.DATA_SENDER_QUEUE_SIZE; i++) {
            emptyJsonBuffers.add(new ByteArrayOutputStream());
        }
        setDaemon(true);
    }

    public void configure(NetworkExportParams params) {
        configurationLock.lock();
        try{
            if(configurationChanged(params)) {
                host = params.getHost();
                port = params.getPort();
                socketTimeout = params.getSocketTimeout();
                reconfigureRequired = true;
            }
        } finally {
            configurationLock.unlock();
        }
    }

    private boolean configurationChanged(NetworkExportParams params) {
        if(host == null) {
            return true; //initial configuration
        } else if(!host.equals(params.getHost())) {
            return true;
        } else if(port != params.getPort()) {
            return true;
        } else if(socketTimeout != params.getSocketTimeout()) {
            return true;
        }
        return false;
    }

    public void shutdown() {
        shutdownRequested = true;
    }

    public ArrayBlockingQueue<ByteArrayOutputStream> getJsonsToSend() {
        return jsonsToSend;
    }

    public ArrayBlockingQueue<ByteArrayOutputStream> getEmptyJsonBuffers() {
        return emptyJsonBuffers;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);

        while(!shutdownRequested) {
            try {
                senderLoop();
            } catch (Throwable e) {
                log.error("Error in DataSender loop: ", e);
                try {
                    Thread.sleep(sleepInterval);
                    if (sleepInterval < 60000) {
                        sleepInterval = sleepInterval * 2;
                    }
                    initalizeConnection();
                } catch (InterruptedException e1) {
                    log.error("Thread interrupted", e1);
                }
            }
        }

        try {
            Runtime.getRuntime().removeShutdownHook(SHUTDOWN_HOOK);
        } catch (IllegalStateException e) {
            /* ignore "Shutdown in progress" */
        }
    }

    private void senderLoop() {
        ArrayList<ByteArrayOutputStream> streams = new ArrayList<ByteArrayOutputStream>(100);
        while (true) {
            if((shutdownRequested && jsonsToSend.isEmpty()) || forceShutdown) {
                closeConnection();
                return;
            } else if(reconfigureRequired) {
                initalizeConnection();
            }

            if (jsonsToSend.drainTo(streams, 100) == 0) {
                try {
                    ByteArrayOutputStream firstBaos = jsonsToSend.poll(1, TimeUnit.SECONDS);
                    if (firstBaos != null) {
                        streams.add(firstBaos);
                    }
                } catch (InterruptedException e) {
                    log.error("Reading from  ArrayBlockingQueue interrupted ", e);
                }

            }
            for (ByteArrayOutputStream baos : streams) {
                if (baos.size() > 2) {
                    sendData(baos);
                }
                baos.reset();
                emptyJsonBuffers.add(baos);
            }
            streams.clear();

            flushIfRequired();

            sleepInterval = 1000;
        }
    }

    private void sendData(ByteArrayOutputStream forSend) {
        try {
            forSend.writeTo(out);
            out.write('\n');
        } catch (IOException e) {
            log.warn("Connection lost. Trying restart socket connection.", e);
            initalizeConnection();
        }
    }

    private void flushIfRequired() {
        long timestamp2 = TimerCache.now;
        if ((timestamp2 - timestamp1) > 5000) {
            try {
                out.flush();
            } catch (IOException e) {
                log.warn("Connection lost. Trying restart socket connection.", e);
                initalizeConnection();
            }
            timestamp1 = timestamp2;
        }
    }

    private void closeConnection() {
        try {
            if (out != null) {
                out.close();
            }
        } catch (Exception e) {}

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {}
    }
}
