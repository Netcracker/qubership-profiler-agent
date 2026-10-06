package com.netcracker.profiler.cloud.transport;

import java.util.*;

public class ESCStopWatch {
    long total = 0;
    long prevStart = -1;

    Collection<ESCStopWatch> watchesToStopWhenStart = Collections.emptyList();
    Collection<ESCStopWatch> watchesToStartWhenStop = Collections.emptyList();

    private Collection<ESCStopWatch> emptyIfNull(ESCStopWatch ...watches){
        if(watches == null || watches .length == 0) {
            return Collections.emptyList();
        }
        return Arrays.asList(watches);
    }

    public void stopWhenStart(ESCStopWatch ...watches) {
        watchesToStopWhenStart = emptyIfNull(watches);
    }

    public void startWhenStop(ESCStopWatch ...watches) {
        watchesToStartWhenStop = emptyIfNull(watches);
    }

    public void start(){
        if(prevStart < 0) {
            for(ESCStopWatch w: watchesToStopWhenStart) {
                w.stop();
            }
            prevStart = nowNanos();
        }
    }
    public void stop() {
        if(prevStart < 0) {
            return;
        }
        total += nowNanos() - prevStart;
        prevStart = -1;
        for(ESCStopWatch w: watchesToStartWhenStop) {
            w.start();
        }
    }

    protected long nowNanos(){
        return System.nanoTime();
    }

    public long getAndReset(){
        stop();
        long result = total;
        total = 0;
        prevStart = -1;
        return result;
    }

    public static ESCStopWatch getWatch(ThreadLocal<ESCStopWatch> tl) {
        ESCStopWatch result = tl.get();
        if(result == null) {
            result = new ESCStopWatch();
            tl.set(result);
        }
        return result;
    }
}
