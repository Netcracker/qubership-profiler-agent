package com.netcracker.cdt.collector.tcp;

import com.netcracker.common.Time;
import com.netcracker.common.models.pod.PodStatus;

import java.io.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static com.netcracker.common.ProtocolConst.*;

public class ConnectionState {
    private final Time time;
    private final ProfilerAgentReader reader;
    private final String addressName;

    final PodStatus pod;

    private final long clientConnectedTime;
    private final AtomicLong agentUptime = new AtomicLong();

    private long lastAccessed;
    private long lastFlushed = -1;
    private long lastDumperFlush = 0L;

    private boolean beingProcessed = false;
    private boolean shutdownRequested = false;

    public ConnectionState(Time time, ProfilerAgentReader reader, String addressName) {
        this.time = time;
        this.reader = reader;
        this.addressName = addressName;
        this.pod = PodStatus.empty(time.now());
        this.clientConnectedTime = time.now().toEpochMilli();
        accessed();
    }

    void setNamespace(String name) {
        pod.setNamespace(name);
    }

    void setMicroservice(String name) {
        pod.setMicroservice(name);
    }

    void setPodName(String originalPodName) {
        pod.setPodName(originalPodName);
    }

    void setClientProtocolVersion(long clientProtocolVersion) {
        pod.setClientProtocolVersion(clientProtocolVersion);
    }

    @Override
    public String toString() {
        var s = addressName;
        if (!pod.isEmpty()) {
            s = pod.screenName() + "|" + s;
        }
        return s;
    }

    void accessed() {
        this.lastAccessed = time.currentTimeMillis();
        this.agentUptime.set(Duration.ofMillis(time.currentTimeMillis() - clientConnectedTime).getSeconds());
    }

    void processed(boolean beingProcessed) {
        this.beingProcessed = beingProcessed;
    }

    void requestShutdown() {
        shutdownRequested = true;
    }

    void flush() {
        lastDumperFlush = time.currentTimeMillis();
        lastFlushed = time.currentTimeMillis();
    }

    public boolean readyForCommand() throws IOException {
        var res = reader.commandAvailable() || shutdownRequested || needsFlushCheck();
//        if (!res) {
//            Log.infof("not ready for command! available? %b , shutdown ? %b , flush ? %b",
//                    reader.commandAvailable(), shutdownRequested, needsFlushCheck());
//        }
        return res;
    }

    public boolean needFlush() {
        return time.currentTimeMillis() - lastDumperFlush > PLAIN_SOCKET_READ_TIMEOUT / 2;
    }

    public boolean needsFlushCheck() {
        return lastFlushed == -1 || time.currentTimeMillis() - lastFlushed > FLUSH_CHECK_INTERVAL_MILLIS;
    }

    public boolean needsShutdown() throws IOException {
        return shutdownRequested;
    }

    public boolean needsExecution() throws IOException {
        return !beingProcessed && readyForCommand();
    }

    public boolean shutdownComplete() {
        return !beingProcessed && reader.isSocketDead();
    }

    public long idleMs() {
        return time.currentTimeMillis() - lastAccessed;
    }

    public boolean isDead() {
        return idleMs() > MAX_IDLE_BEFORE_DEATH;
    }

    public boolean timeToKill() {
        return idleMs() > 2 * MAX_IDLE_BEFORE_DEATH;
    }


}
